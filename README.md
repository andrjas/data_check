# data_check

data_check is a simple data validation tool. In its most basic form it will execute SQL queries and compare the results against CSV or Excel files. But there are more advanced features:

## Features

* [CSV checks](#csv-checks): compare SQL queries against CSV files
* Excel support: Use Excel (xlsx) instead of CSV
* multiple environments (databases) in the configuration file
* populate tables from CSV files
* execute any SQL files on a database
* more complex [pipelines](#pipelines)
* run any script/command (via pipelines)
* simplified checks for empty datasets and full table comparison
* lookups to reuse the same data in multiple queries

## Database support

data_check should work with any database that works with [SQLAlchemy](https://docs.sqlalchemy.org/en/14/dialects/). Currently data_check is tested against PostgreSQL, MySQL, SQLite, Oracle and Microsoft SQL Server.

## Quickstart

You need Python 3.7.1 or above to run data_check. The easiest way to install data_check is via [pipx](https://github.com/pipxproject/pipx):

`pipx install data-check`

The data_check Git repository is also a sample data_check project. Clone the repository, switch to the folder and run data_check:

```
git clone git@github.com:andrjas/data_check.git
cd data_check/example
data_check
```

This will run the tests in the _checks_ folder using the default connection as set in data_check.yml.

See the [documentation](https://andrjas.github.io/data_check) how to install data_check in different environments with additional database drivers and other usages of data_check.

## Project layout

data_check has a simple layout for projects: a single configuration file and a folder with the test files. You can also organize the test files in subfolders.

    data_check.yml    # The configuration file
    checks/           # Default folder for data tests
        some_test.sql # SQL file with the query to run against the database
        some_test.csv # CSV file with the expected result
        subfolder/    # Tests can be nested in subfolders

## CSV checks

This is the default mode when running data_check. data_check expects a SQL file and a CSV file. The SQL file will be executed against the database and the result is compared with the CSV file. If they match, the test is passed, otherwise it fails.

## Pipelines

If data_check finds a file named _data\_check\_pipeline.yml_ in a folder, it will treat this folder as a pipeline check. Instead of running [CSV checks](#csv-checks) it will execute the steps in the YAML file.

Example project with a pipeline:

    data_check.yml
    checks/
        some_test.sql                # this test will run in parallel to the pipeline test
        some_test.csv
        sample_pipeline/
            data_check_pipeline.yml  # configuration for the pipeline
            data/
                my_schema.some_table.csv       # data for a table
            data2/
                some_data.csv        # other data
            some_checks/             # folder with CSV checks
                check1.sql
                check1.csl
                ...
            run_this.sql             # a SQL file that will be executed
            cleanup.sql
        other_pipeline/              # you can have multiple pipelines that will run in parallel
            data_check_pipeline.yml
            ...

The file _sample\_pipeline/data\_check\_pipeline.yml_ can look like this:

```yaml
steps:
    # this will truncate the table my_schema.some_table and load it with the data from data/my_schema.some_table.csv
    - load_tables: data
    # this will execute the SQL statement in run_this.sql
    - sql_file: run_this.sql
    # this will append the data from data2/some_data.csv to my_schema.other_table
    - load:
        file: data2/some_data.csv
        table: my_schema.other_table
        load_mode: append
    # this will run a python script and pass the connection name
    - cmd: "python3 /path/to/my_pipeline.py --connection {{CONNECTION}}"
    # this will run the CSV checks in the some_checks folder
    - check: some_checks
```

Pipeline checks and simple CSV checks can coexist in a project.

## Documentation

See the [documentation](https://andrjas.github.io/data_check) how to setup data_check, how to create a new project and more options.
