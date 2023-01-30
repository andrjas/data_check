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
    from data_check.sql import Table, ColumnInfo

from ..date import fix_date_dtype
from ..file_ops import expand_files, read_csv
from ..utils.deprecation import deprecated_method_argument
from .load_mode import LoadMode


class TableLoader:
    """
    Helper class that implements the methods to load a table from a CSV file.
    """

    def __init__(
        self, sql: DataCheckSql, output: DataCheckOutput, default_load_mode: LoadMode
    ):
        self.sql = sql
        self.output = output
        self.default_load_mode = default_load_mode

    def __del__(self):
        self.sql.disconnect()

    def _prepare_table_for_load(self, table: Table, mode: LoadMode):
        if mode == LoadMode.TRUNCATE:
            table.truncate_if_exists()

    @staticmethod
    def _load_mode_to_pandas_if_exists(
        mode: LoadMode,
    ) -> Literal["fail", "replace", "append"]:
        if mode == LoadMode.REPLACE:
            return "replace"
        return "append"

    def pre_insert(self, connection: Connection, table: Table):
        if self.sql.dialect == "mssql":
            # When appending/upserting data into a table with identity columns,
            # we need to enable IDENTITY_INSERT to allow inserting explicit values
            # into these columns.
            if table.exists():
                if table.sql_table.primary_key:
                    connection.execute(f"SET IDENTITY_INSERT {table} ON")

    def upsert_data(self, data: pd.DataFrame, table: Table) -> bool:
        other_columns = [
            c for c in data.columns.to_list() if c not in table.primary_keys
        ]

        sql_table = table.sql_table
        update_stmt = sql_table.update()
        insert_stmt = sql_table.insert()

        for p in table.primary_keys:
            update_stmt = update_stmt.where(sql_table.c[p] == bindparam(f"_{p}"))

        update_stmt = update_stmt.values(**{oc: bindparam(oc) for oc in other_columns})
        connection = self.sql.get_connection()
        self.pre_insert(connection, table)

        for _, row in data.iterrows():
            row_as_dict = cast(Dict[str, Any], row.to_dict())
            for p in table.primary_keys:
                row_as_dict[f"_{p}"] = row_as_dict.pop(p)
            rows = connection.execute(update_stmt, **row_as_dict)
            if rows.rowcount == 0:
                connection.execute(insert_stmt, **cast(Dict[str, Any], row.to_dict()))
        return False

    def load_table(
        self, table: Table, data: pd.DataFrame, mode: LoadMode, dtype=None
    ) -> bool:
        self._prepare_table_for_load(table, mode)
        if_exists = self._load_mode_to_pandas_if_exists(mode=mode)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # ignore SADeprecationWarning
            fix_date_dtype(data, dtype)
            if not dtype:
                # dtype can be {}, but for to_sql it's best to use None then
                dtype = None
            if mode == LoadMode.UPSERT:
                return self.upsert_data(data, table)
            else:
                connection = self.sql.get_connection()
                self.pre_insert(connection, table)
                data.to_sql(
                    name=table.name,
                    schema=table.schema,
                    con=connection,
                    if_exists=if_exists,
                    index=False,
                    dtype=dtype,
                )
                return True

    def get_load_mode(self, deprecated_load_mode, mode) -> LoadMode:
        if deprecated_load_mode is not None:
            if isinstance(deprecated_load_mode, str):
                mode = TableLoader.load_mode_from_string(deprecated_load_mode)
            else:
                mode = deprecated_load_mode
        if isinstance(mode, str):
            mode = TableLoader.load_mode_from_string(mode)
        if mode == LoadMode.DEFAULT:
            mode = self.default_load_mode
        return mode

    def load_table_from_file(
        self,
        table: str,
        file: Path,
        mode: Union[str, LoadMode] = LoadMode.DEFAULT,
        base_path: Path = Path("."),
        load_mode: Union[str, LoadMode, None] = None,
    ):
        from data_check.sql import Table

        deprecated_method_argument(load_mode, mode, LoadMode.DEFAULT)
        mode = self.get_load_mode(load_mode, mode)
        rel_file = base_path / file
        _table = Table.from_table_name(self.sql, table)
        data = self.load_df_from_file(rel_file, _table.column_info)

        result = self.load_table(
            table=_table,
            data=data,
            mode=mode,
            dtype=_table.column_info.dtypes,
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
        mode: Union[str, LoadMode] = LoadMode.DEFAULT,
        base_path: Path = Path("."),
        load_mode: Union[str, LoadMode, None] = None,
    ):
        deprecated_method_argument(load_mode, mode, LoadMode.DEFAULT)
        mode = self.get_load_mode(load_mode, mode)
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
