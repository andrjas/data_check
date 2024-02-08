from typing import List, Sequence, Union

from sqlalchemy import Row

from data_check.sql import DataCheckSql


class DataCheckSqlDatabricks(DataCheckSql):
    def use_parameters(self) -> bool:
        # cannot use bindparams due to https://github.com/databricks/databricks-sql-python/pull/267
        return False

    def prepare_result(
        self, res: Sequence[Row], columns: List[str]
    ) -> Union[bool, Sequence[Row]]:
        # Databricks returns an empty list and a single column 'Result'
        # if the query is a DML/DDL. That's why we convert it to True
        # in this case to indicate successful execution.
        if res == [] and columns == ["Result"]:
            return True
        return res
