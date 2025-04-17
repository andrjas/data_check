from functools import cached_property
from typing import Any, Optional

import pandas as pd
from sqlalchemy import event
from sqlalchemy.engine import Engine

from data_check.sql import DataCheckSql
from data_check.sql.table import Table
from data_check.sql.table_loader import TableLoader


class TableLoaderOracle(TableLoader):
    def prepare_dtypes(
        self, data: pd.DataFrame, table: Table, dtype
    ) -> Optional[dict[str, Any]]:
        # when creating a new table in Oracle with FLOAT types,
        # we need to specify the binary_precision
        float_types = {
            k: v for k, v in dict(data.dtypes).items() if v in ("float32", "float64")
        }
        if any(float_types) and not table.exists():
            from sqlalchemy.dialects import oracle
            from sqlalchemy.sql.sqltypes import Float

            for col_name, dt in float_types.items():
                # use same precision as in pandas' _sqlalchemy_type
                precision = 23 if dt == "float32" else 53
                binary_precision = 76 if dt == "float32" else 126
                dtype[col_name] = Float(precision=precision).with_variant(
                    oracle.FLOAT(binary_precision=binary_precision), "oracle"
                )

        return super().prepare_dtypes(data, table, dtype)


class DataCheckSqlOracle(DataCheckSql):
    @cached_property
    def table_loader(self) -> TableLoader:
        return TableLoaderOracle(self, self.output, self.config.default_load_mode)

    def post_get_engine_hook(self, engine: Engine):
        self.register_setinputsizes_event(engine)

    def register_setinputsizes_event(self, engine: Engine):
        """
        Do not use CLOB for loading strings (and large decimals)
        https://docs.sqlalchemy.org/en/20/dialects/oracle.html#example-2-remove-all-bindings-to-clob
        """
        try:
            from cx_Oracle import CLOB  # type: ignore
        except ImportError:
            CLOB = None  # noqa: N806

        @event.listens_for(engine, "do_setinputsizes")
        def _remove_clob(inputsizes, cursor, statement, parameters, context):
            _ = cursor
            _ = statement
            _ = parameters
            _ = context
            for bindparam, dbapitype in list(inputsizes.items()):
                if dbapitype is CLOB:
                    del inputsizes[bindparam]
