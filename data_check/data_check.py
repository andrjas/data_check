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

    def run(self, any_path: Path) -> bool:
        """
        Run a data_check test on a file or folder.
        In case of a file, it will just call run_test.
        For a folder, it will scan the folder recursively and run a test for each input file.

        Returns True if all tests passed, otherwise False.
        """
        if any_path.is_file():
            return self.run_test(any_path)
        elif any_path.is_dir():
            all_files = any_path.glob("**/*.sql")
            return all([self.run_test(sql_file) for sql_file in all_files])
        else:
            raise Exception(f"unexpected path: {any_path}")
