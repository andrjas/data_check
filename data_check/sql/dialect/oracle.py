from sqlalchemy import event
from sqlalchemy.engine import Engine

from data_check.sql import DataCheckSql


class DataCheckSqlOracle(DataCheckSql):
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
            CLOB = None

        @event.listens_for(engine, "do_setinputsizes")
        def _remove_clob(inputsizes, cursor, statement, parameters, context):
            _ = cursor
            _ = statement
            _ = parameters
            _ = context
            for bindparam, dbapitype in list(inputsizes.items()):
                if dbapitype is CLOB:
                    del inputsizes[bindparam]
