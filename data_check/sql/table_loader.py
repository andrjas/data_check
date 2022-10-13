from __future__ import annotations

import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union, cast

import pandas as pd
from sqlalchemy.engine import Connection
from sqlalchemy.sql import text
from sqlalchemy.sql.expression import bindparam

if TYPE_CHECKING:
    from data_check.sql import DataCheckSql
    from data_check.output import DataCheckOutput
    from data_check.sql.table_info import ColumnInfo

from ..date import fix_date_dtype
from ..io import expand_files, read_csv
from ..utils.deprecation import deprecated_method_argument
from .load_mode import LoadMode


class TableLoader:
    """
    Helper class that implements the methods to load a table from a CSV file.
    """

    def __init__(self, sql: DataCheckSql, output: DataCheckOutput):
        self.sql = sql
        self.output = output

    def __del__(self):
        self.sql.disconnect()

    def drop_table_if_exists(self, table_name: str, schema: Optional[str]):
        if self.sql.table_info.table_exists(table_name, schema):
            if schema:
                drop_stmt = f"DROP TABLE {schema}.{table_name}"
            else:
                drop_stmt = f"DROP TABLE {table_name}"
            self.sql.get_connection().execute(
                text(drop_stmt).execution_options(autocommit=True)
            )

    def _truncate_statement(self, table_name: str) -> str:
        if self.sql.dialect == "sqlite":
            return f"DELETE FROM {table_name}"
        return f"TRUNCATE TABLE {table_name}"

    def _prepare_table_for_load(self, table_name: str, mode: LoadMode):
        if mode == LoadMode.TRUNCATE:
            schema, name = self.sql.table_info.parse_table_name(table_name)
            if self.sql.table_info.table_exists(table_name=name, schema=schema):
                self.sql.get_connection().execute(
                    text(self._truncate_statement(table_name)).execution_options(
                        autocommit=True
                    )
                )

    @staticmethod
    def _load_mode_to_pandas_if_exists(
        mode: LoadMode,
    ) -> Literal["fail", "replace", "append"]:
        if mode == LoadMode.REPLACE:
            return "replace"
        return "append"

    def pre_insert(self, connection: Connection, name: str, schema: Optional[str]):
        if self.sql.dialect == "mssql":
            # When appending/upserting data into a table with identity columns,
            # we need to enable IDENTITY_INSERT to allow inserting explicit values
            # into these columns.
            if self.sql.table_info.table_exists(name, schema):
                table = self.sql.table_info.get_table(name, schema)
                if table.primary_key:
                    connection.execute(f"SET IDENTITY_INSERT {table} ON")

    def upsert_data(self, data: pd.DataFrame, name: str, schema: Optional[str]) -> bool:
        pk = self.sql.table_info.get_primary_keys(name, schema)
        other_columns = [c for c in data.columns.to_list() if c not in pk]

        table = self.sql.table_info.get_table(name, schema)
        update_stmt = table.update()
        insert_stmt = table.insert()

        for p in pk:
            update_stmt = update_stmt.where(table.c[p] == bindparam(f"_{p}"))

        update_stmt = update_stmt.values(**{oc: bindparam(oc) for oc in other_columns})
        connection = self.sql.get_connection()
        self.pre_insert(connection, name, schema)

        for _, row in data.iterrows():
            row_as_dict = cast(Dict[str, Any], row.to_dict())
            for p in pk:
                row_as_dict[f"_{p}"] = row_as_dict.pop(p)
            rows = connection.execute(update_stmt, **row_as_dict)
            if rows.rowcount == 0:
                connection.execute(insert_stmt, **cast(Dict[str, Any], row.to_dict()))
        return False

    def load_table(
        self, table_name: str, data: pd.DataFrame, mode: LoadMode, dtype=None
    ) -> bool:
        self._prepare_table_for_load(table_name, mode)
        if_exists = self._load_mode_to_pandas_if_exists(mode=mode)
        schema, name = self.sql.table_info.parse_table_name(table_name)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # ignore SADeprecationWarning
            fix_date_dtype(data, dtype)
            if not dtype:
                # dtype can be {}, but for to_sql it's best to use None then
                dtype = None
            if mode == LoadMode.UPSERT:
                return self.upsert_data(data, name, schema)
            else:
                connection = self.sql.get_connection()
                self.pre_insert(connection, name, schema)
                data.to_sql(
                    name=name,
                    schema=schema,
                    con=connection,
                    if_exists=if_exists,
                    index=False,
                    dtype=dtype,
                )
                return True

    @staticmethod
    def get_load_mode(deprecated_load_mode, mode) -> LoadMode:
        if deprecated_load_mode is not None:
            if isinstance(deprecated_load_mode, str):
                mode = TableLoader.load_mode_from_string(deprecated_load_mode)
            else:
                mode = deprecated_load_mode
        if isinstance(mode, str):
            mode = TableLoader.load_mode_from_string(mode)
        return mode

    def load_table_from_file(
        self,
        table: str,
        file: Path,
        mode: Union[str, LoadMode] = LoadMode.TRUNCATE,
        base_path: Path = Path("."),
        load_mode: Union[str, LoadMode, None] = None,
    ):
        deprecated_method_argument(load_mode, mode, LoadMode.TRUNCATE)
        mode = TableLoader.get_load_mode(load_mode, mode)
        rel_file = base_path / file
        column_info = self.sql.table_info.get_column_info(table)
        data = self.load_df_from_file(rel_file, column_info)

        result = self.load_table(
            table_name=table,
            data=data,
            mode=mode,
            dtype=column_info.dtypes,
        )
        if result:
            self.output.print(f"table {table} loaded from {rel_file}")
        else:
            self.output.print(f"loading table {table} from {rel_file} failed")
        return result

    def load_df_from_file(self, file: Path, column_info: ColumnInfo) -> pd.DataFrame:
        if file.suffix.lower() == ".csv":
            data = read_csv(
                csv_file=file,
                parse_dates=column_info.date_column_names,
                string_columns=column_info.string_column_names,
            )
        elif file.suffix.lower() == ".xlsx":
            data = pd.read_excel(
                file, sheet_name=0, header=0, engine="openpyxl", dtype="object"
            )
        else:
            raise Exception(f"file type unsupported: {file.suffix.lower()}")
        return data

    def load_tables_from_files(
        self,
        files: List[Path],
        mode: Union[str, LoadMode] = LoadMode.TRUNCATE,
        base_path: Path = Path("."),
        load_mode: Union[str, LoadMode, None] = None,
    ):
        deprecated_method_argument(load_mode, mode, LoadMode.TRUNCATE)
        mode = TableLoader.get_load_mode(load_mode, mode)
        flat_files = expand_files(
            files, extension=[".csv", ".xlsx"], base_path=base_path
        )
        parameters = [{"table": f.stem, "file": f, "mode": mode} for f in flat_files]
        results = self.sql.runner.run_any(
            run_method=self.load_table_from_file, parameters=parameters
        )
        return all(results)

    @staticmethod
    def load_mode_from_string(lm_str: str) -> LoadMode:
        return LoadMode.from_string(lm_str)
