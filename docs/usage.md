# Usage

## Commands

* `data_check run` - [Run checks (default command)](#run).
* `data_check fake` - [Generate test data](#fake).
* `data_check gen` -  [Generate CSV files from query files](#gen).
* `data_check load` - [Load data from files into tables](#load).
* `data_check ping` - [Tries to connect to the database](#ping).
* `data_check sql` - [Run SQL statements](#sql).

## Common options

Common options can be used with any command.

* `-c/--connection CONNECTION` - Use another connection than the default.
* `-n/--workers WORKERS` - Use WORKERS threads to run the queries (default: 4).
* `--config CONFIG` - Config file to use (default: data_check.yml).
* `--quiet` - Do not print any output.
* `--verbose` - Print verbose output.
* `--traceback` - Print traceback output for debugging.
* `--log LOGFILE` - Write output to a log file.
* `--version` - Show the version and exit.
* `--help` - Show this message and exit.

## run

`run` executes checks against a database. It is the default command and can be omitted.

### Options

* `--print` - Print failed results data.
* `--print-format FORMAT` - Format for printing failed results (csv (default), pandas, json).
* `--print-json` - Shortcut for "--print --print-format json".
* `--diff` - Print only the different columns for failed results. Use with --print.


### Examples

* `data_check run` - Run data_check against the default connection in the _checks_ folder.
* `data_check` - Same as `data_check run`
* `data_check run some_folder` - Run data_check against the default connection in the _some\_folder_ folder.
* `data_check run some_folder/some_file.sql` - Run data_check against the default connection for a single test.
* `data_check run --print` - Run data_check against the default connection in the  _checks_ folder and prints all failed result data.
* `data_check run --print --diff some_folder` - Run data_check against the default connection in the _some\_folder_ folder and prints only the different columns for failed results.

## fake

`fake` generates test data for existing tables and writes it into CSV files. The configuration file is described in [Test data](test_data.md).

### Options

* `-o/--output PATH` - Output path for the CSV file.
* `--force` - Overwrite existing files.


### Examples

* `data_check fake fake_config.yml` - Generates test data as defined in _fake\_config.yml_ and writes it into a CSV file with the same name as the table name.
* `data_check fake fake_config.yml --output fake.csv` - Generates test data as defined in _fake\_config.yml_ and writes it into _fake.csv_.
* `data_check fake fake_config.yml --force` - Generates test data as defined in _fake\_config.yml_, overwrites existing CSV files.
* `data_check fake fake_config.yml fake_config2.yml` - Generates test data for both config files.


## gen

`gen` generates the expectation files (CSV).

### Options

* `--force` - Overwrite existing files.

### Examples

* `data_check gen` - Generates all missing expectation files in the _checks_ folder.
* `data_check gen --force some_folder` - Generates and overwrites all expectation files in the _some\_folder_ folder.


## load

`load` is used to load data from files (CSV or Excel) into database tables. By default it will load into a table with the same name as the file and truncate the table before loading. If the table does not exists, it will create the table with a column definition guessed from the file content.

### Options

* `--table`-  Table name to load data into
* `--mode` -  How to load the table: truncate (default), append or replace.

### Examples

* `data_check load some_path/schema.table.csv` - Truncates the table schema.table and loads the csv from _some\_path/schema.table.csv_ into the table.
* `data_check load --table schema.other_table some_path/schema.table.csv` - Same as above but uses the table schema.other_table. Only works for a single CSV/Excel file.
* `data_check load --mode append some_path/schema.table.csv` - Loads the csv from _some\_path/schema.table.csv_ into the table, appends only without touching the existing data in the table.
* `data_check load --mode replace some_path/schema.table.csv` - Drops and recreates the table schema.table and loads the csv from _some\_path/schema.table.csv_ into the table.
* `data_check load some_path/schema.table.csv other_path/schema2.other_table.csv` - Loads multiple tables from multiple files.
* `data_check load some_path` - Loads all CSV/Excel files from _some\_path_ each into the same table as the file name.

## ping

`ping` tries to connect to the database. The exit code is 0 if it successful, otherwise it is 1.
`ping` doesn't have any special options, except the common ones.

### Examples

* `data_check ping` - Tries to connect to the default database.
* `data_check ping --connection test2` - Tries to connect to the database with the connection _test2_.
* `data_check ping --quiet` - Tries to connect to the default database. Doesn't print anything.


## sql

`sql` runs any SQL query/command against the database. The query can be passed as an argument or from a file. The result of the query can be written into a CSV file.

### Options

* `--file, --file` - Run any SQL script in a list of SQL files.
* `-o/--output PATH` - Output path for the result.
* `-W/--write-check PATH` - Create a check from the sql statement (SQL and CSV
                          file).

### Examples

* `data_check sql "select * from some.table"` - Runs the given query against the default database connection.
* `data_check sql --connection test2 "select * from some.table"` - Runs the given query against the database connection for _test2_.
* `data_check sql --file some/file.sql` - Runs the query in the file against the default database connection.
* `data_check sql --files some/path` - Runs the queries from all SQL files in _some/path_ against the default database connection. The queries are run in parallel, the order is random.
* `data_check sql --workers 1 --files some/path` - Runs the queries from all SQL files in _some/path_ against the default database connection. The queries are run sequentially ordered by the path/file name.

## Exit codes

Possible exit codes:

__Exit code 0:__ All tests run successfully.<br/>
__Exit code 1:__ At least one test failed.


## Logging

`--log` will write the output to a log file and to the console. With `--quiet` the output will only be printed to the log file. If the file exists, the output will be appended.

You can also set the log file in data_check.yml:

```yaml
log: logfile.txt
```

Failed results and tracebacks are always written to the log file, even when the parameters `--print` and `--traceback` are not used.


## Loading data into tables

Sometimes you need to populate tables with some data before running pipeline tests. With data_check you can use CSV or Excel files to load data into tables. The [CSV format](usage.md#csv-format) is the same as used for testing. The header in the CSV file or the first row in the Excel file must match the columns in the table. Additionally, Excel cells can use its native date/datetime format.

If the table doesn't exist, it will be created. The schema and table names are always case-insensitive, as long as the database supports it. Otherwise, they are lowercased.

### Loading data into a single table

To load data from some CSV or Excel file into a table, you can use `data_check load path/to/some_file.[csv/xlsx] --table schema.table_name`. This will truncate the table and load the content of the file into the table. You can specify different [load modes](usage.md#load-modes) if you do not want to truncate the table.

### Loading data into multiple tables

You can use multiple CSV and Excel files or a whole folder to load the data into tables. The table name will be derived from the file name.

* `data_check load path/schema.table_1.csv` - this will load the data from the CSV file into the table schema.table_1
* `data_check load path/schema.table_2.xlsx` - this will load the data from the Excel file into the table schema.table_2
* `data_check load path/to/some_folder` - this will load the data from all CSV and Excel files in this folder into tables matching the file names.

The path will be searched recursively for CSV and Excel files. The folder structure doesn't matter when matching the table names, only the file name matters.

### Load modes

There are multiple modes that control the data loading:

* truncate - truncate the table before loading the data.
* replace - drop the table and recreate it based on the columns in the CSV or Excel file.
* append - do not touch the table before loading the data.

By default, the tables will be truncated before loading the data. You can use the other modes with `--load-mode`:

`data_check load file.csv --table table_name --mode replace`
`data_check load path/to/tables_folder --mode append`

### CSV and data types

When loading data from CSV files, data_check (or more precisely: [pandas](https://pandas.pydata.org/)) will infer the data types from the file.
When loading the data into the table, the database will usually implicitly convert the data types.
This works good for simple data types like strings and numbers.

If you need to load date types (or timestamps) and the table has a date column, data_check will try to convert these columns in the CSV file into a datetime.
This doesn't work when using `--load-mode replace` since the table will be dropped before it can be analyzed. This will probably result in a varchar column instead of date.

Use [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) for dates, like for [CSV checks]((csv_checks.md#csv-format)

## Executing arbitrary SQL code

You can run any SQL file against the database by using the `--sql-files` command:

`data_check sql --files sql_file.sql other_file.sql`

or a whole folder recursively:

`data_check sql --files some/folder/with/sql_files`

All files are run in parallel. If you have dependencies between the files, data_check must be called sequentially for each file.

Multiple statements in a SQL file usually must be inside an anonymous block. MySQL doesn't support this however.

`--file` is an alias for `--files`. Use it to indicate, that you only want to run a single file.

You can also run a SQL statement directly from the command line:

`data_check sql "select * from tableX"`

This will execute the query and, if it is a query, print the result as CSV. You can also write the result into a file:

`data_check sql "select * from tableX" --output some_file.csv`

You can use both [templates](csv_checks.md#templates) and [lookups](csv_checks.md#lookups) with `sql`.

### Generating a CSV check from a SQL statement

You can use `sql` to create a CSV check:

`data_check sql "select * from tableX" --write-check some_check.sql`

This writes the SQL statement into `some_check.sql` and the result as CSV into `some_check.csv`.
