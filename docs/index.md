# Welcome to data_check

data_check is a simple data validation tool. In its most basic form, it will execute SQL queries and compare the results against CSV or Excel files. But there are more advanced features:

## Features

* [CSV checks](csv_checks.md): compare SQL queries against CSV files
* Excel support: Use Excel (xlsx) instead of CSV
* multiple environments (databases) in the configuration file
* [populate tables](loading_data.md) from CSV or Excel files
* [execute any SQL files on a database](sql.md)
* more complex [pipelines](pipelines.md)
* run any script/command (via pipelines)
* simplified checks for [empty datasets](csv_checks.md#empty-dataset-checks) and [full table comparison](csv_checks.md#full-table-checks)
* [lookups](csv_checks.md#lookups) to reuse the same data in multiple queries
* [test data generation](test_data.md)

## Database support

data_check should work with any database that works with [SQLAlchemy](https://docs.sqlalchemy.org/en/14/dialects/). Currently data_check is tested against PostgreSQL, MySQL, SQLite, Oracle and Microsoft SQL Server.

## Why?

data_check tries to solve a simple problem in data domains: during development you create multiple SQL queries to validate your data and compare the result manually. With data_check you can organize your data tests and run them automatically, for example in a CI pipeline.

## How to get started

First [install](install.md) data_check. You can then try data_check with the [sample project](example.md#data_check-sample-project).

To create a new project folder write a _data\_check.yml_ for the [configuration](index.md#configuration) and put your [test files](index.md#test-files) in the _checks_ folder.

## Project layout

data_check has a simple layout for projects: a single configuration file and a folder with the test files. You can also organize the test files in subfolders.

    data_check.yml          # The configuration file
    checks/                 # Default folder for data tests
        some_test.sql       # SQL file with the query to run against the database
        some_test.csv       # CSV file with the expected result
        other_test.sql      # SQL file with another test
        other_test.xlsx     # Expected result for other_test.sql in an Excel file
        empty_result.sql    # SQL file with a result set that is expected to be empty
        empty_result.empty  # empty file for empty_result.sql
        subfolder/          # Tests can be nested in subfolders

## Configuration

data_check uses the _data\_check.yml_ file in the current folder for configuration.
This is a simple YAML file:

```yaml
default_connection: con

connections:
    con: connection-string
```

Under _connections_ you can put multiple [connection strings](https://docs.sqlalchemy.org/en/14/core/engines.html) with names. _default_connection_ is the connection name that data_check uses when no additional arguments are given. You can also use environment variables in the connection string to store the credentials outside _data\_check.yml_ (e.g. `postgresql://postgres:${POSTGRES_PWD}@db:5432`).

## Test files

The test files are organized in the _checks_ folder and its subfolders. data_check will run the queries for each SQL file in these folders and compare the results with the corresponding CSV files. The CSV file must be named exactly like the SQL file, only with different file endings.

Instead of writing the CSV files manually, you can also [generate](usage.md#generating-expectation-files) them from the SQL files.

## How it works

data_check uses [pandas](https://pandas.pydata.org/) and [SQLAlchemy](https://www.sqlalchemy.org/) internally. The SQL files are executed and converted to DataFrames. The CSV files are also parsed to DataFrames. Both DataFrames are then merged with a full outer join. If some rows differ, the test is considered as failed.

## License

[MIT](https://github.com/andrjas/data_check/blob/main/LICENSE)
