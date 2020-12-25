# data_check

data_check is a simple data validation tool. Write SQL queries and CSV files with the expected result sets and data_check will test the result sets against the queries.

data_check should work with any database that works with [SQLAlchemy](https://docs.sqlalchemy.org/en/13/dialects/). Currently data_check is tested against PostgreSQL, MySQL, SQLite, Oracle and Microsoft SQL Server.

## Quickstart

You need Python 3.6 or above to run data_check. The easiest way to install data_check is via [pipx](https://github.com/pipxproject/pipx):

`pipx install data_check`

The data_check Git repository is also a sample data_check project. Clone the repository, switch to the folder and run data_check:

```
git clone git@github.com:andrjas/data_check.git
cd data_check
data_check
```

This will run the tests in the _checks_ folder using the default connection as set in data_check.yml.

See the [documentation](https://andrjas.github.com/data_check) how to install data_check in different environments with additional database drivers and other usages of data_check.

## Project layout

data_check has a simple layout for projects: a single configuration file and a folder with the test files. You can also organize the test files in subfolders.

    data_check.yml    # The configuration file
    checks/           # Default folder for data tests
        some_test.sql # SQL file with the query to run against the database
        some_test.csv # CSV file with the expected result
        subfolder/    # Tests can be nested in subfolders

## Documentation

See the [documentation](https://andrjas.github.com/data_check) how to setup data_check, how to create a new project and more options.
