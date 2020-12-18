from pathlib import Path
import yaml
import pandas as pd


class DataCheck:
    """
    Main class for data_check.
    """

    @staticmethod
    def read_config(config_path: Path = Path("data_check.yml")):
        config = yaml.safe_load(config_path.open())
        return config

    def __init__(self, connection: str):
        self.connection = connection

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Run a query on the database and return a Pandas DataFrame with the result.
        """
        return pd.read_sql(query, self.connection)

    def get_expect_file(self, sql_file: Path) -> Path:
        """
        Returns the csv file with the expected results for a sql file.
        """
        return sql_file.parent / (sql_file.stem + ".csv")

    def run_test(self, sql_file: Path) -> bool:
        """
        Run a data_check test on a single input file.
        Returns True if the test passed, otherwise False.
        """
        expect_file = self.get_expect_file(sql_file)
        sql_result = self.run_query(sql_file.read_text())
        expect_result = pd.read_csv(expect_file)
        df_diff = sql_result.merge(expect_result, indicator=True, how="outer")

        if len(df_diff[df_diff._merge != "both"]) != 0:
            print(f"{sql_file}: FAILED")
            return False
        else:
            print(f"{sql_file}: PASSED")
            return True
