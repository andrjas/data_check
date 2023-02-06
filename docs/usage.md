# Usage

## Commands

* `data_check run` - [Run checks (default command)](#run).
* `data_check fake` - [Generate test data](#fake).
* `data_check gen` -  [Generate CSV files from query files](#gen).
* `data_check load` - [Load data from files into tables](#load).
* `data_check append` - [Append data from files into tables](#append).
* `data_check ping` - [Tries to connect to the database](#ping).
* `data_check sql` - [Run SQL statements](#sql).

## Common options

Common options can be used with any command.

* `-c/--connection CONNECTION` - Use another connection than the default.
* `-n/--workers WORKERS` - Use WORKERS threads to run the queries (default: 4).
* `--config CONFIG` - Config file to use (default: _data\_check.yml_).
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

* `--table` -  Table name to load data into
* `--mode` -  How to load the table: truncate (default), append or replace.

### Examples

* `data_check load some_path/schema.table.csv` - Truncates the table schema.table and loads the csv from _some\_path/schema.table.csv_ into the table.
* `data_check load --table schema.other_table some_path/schema.table.csv` - Same as above but uses the table schema.other_table. Only works for a single CSV/Excel file.
* `data_check load --mode append some_path/schema.table.csv` - Loads the csv from _some\_path/schema.table.csv_ into the table, appends only without touching the existing data in the table.
* `data_check load --mode replace some_path/schema.table.csv` - Drops and recreates the table schema.table and loads the csv from _some\_path/schema.table.csv_ into the table.
* `data_check load some_path/schema.table.csv other_path/schema2.other_table.csv` - Loads multiple tables from multiple files.
* `data_check load some_path` - Loads all CSV/Excel files from _some\_path_ each into the same table as the file name.

## append

`append` is an alias for `load --mode append`.

### Options
* `--table` -  Table name to load data into

### Examples

See [load](#load) examples.

## ping

`ping` tries to connect to the database. The exit code is 0 if it successful, otherwise it is 1.
With `--wait` `ping` will retry to connect to the database until the timeout is reached.

### Options

* `--wait` - retry and wait until timeout is reached
* `--timeout SECONDS` - timeout for wait in seconds (default: 5)
* `--retry SECONDS` - retry for wait in seconds (default: 1)

### Examples

* `data_check ping` - Tries to connect to the default database.
* `data_check ping --connection test2` - Tries to connect to the database with the connection _test2_.
* `data_check ping --quiet` - Tries to connect to the default database. Doesn't print anything.
* `data_check ping --wait` - Tries to connect to the default database for 5 seconds, retrying each second.
* `data_check ping --wait --timeout 60` - Tries to connect to the default database for 60 seconds, retrying each second.
* `data_check ping --wait --timeout 60 --retry 5` - Tries to connect to the default database for 60 seconds, retrying each 5 seconds.


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

You can also set the log file in _data\_check.yml_:

```yaml
log: logfile.txt
```

Failed results and tracebacks are always written to the log file, even when the parameters `--print` and `--traceback` are not used.

## Environment variables

The environment variable `DATA_CHECK_CONNECTION` can be used to override the default connection.

The precedence is:
1. Value from CLI
2. Value from environment variables
3. Default values (from the config file)
