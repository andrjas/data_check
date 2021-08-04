# Usage

## Commands

* `data_check` - Run data_check agains the default connection in the _checks_ folder.
* `data_check some_folder` - Run data_check agains the default connection in the _some_folder_ folder.
* `data_check some_folder/some_file.sql` - Run data_check against the default connection for a single test.
* `data_check -c/--connection CONNECTION` - use another connection than the default.
* `data_check -n/--workers WORKERS` - use WORKERS threads to run the queries (default: 4).
* `data_check --print` - print failed results data.
* `data_check --print-format FORMAT` - format for printing failed results (pandas, csv).
* `data_check --print-csv` - shortcut for "--print --print-format csv".
* `data_check --print-json` - shortcut for "--print --print-format json".
* `data_check -g/--gen/--generate` - generate expectation files if they don't exist.
* `data_check --force` - when set, --generate will overwrite files.
* `data_check --config CONFIG` - config file to use (default: data_check.yml).
* `data_check --load PATH --table`-  load table data from a csv into the table
* `data_check --load-mode MODE` -  how to load the table: truncate (default), append or replace. Use with --load or --load-tables.
* `data_check --load-tables some_folder/or/some_file.csv`-  load tables from a list of csv files
* `data_check --run-sql some_folder/or/some_file.sql` - run any SQL script in a list of SQL files
* `data_check --sql "SQL statement"` - run any SQL statement. Print result as CSV if it is a query.
* `data_check -o/--output PATH` - output path for --sql.
* `data_check --ping` - tries to connect to the database.
* `data_check --verbose` - print verbose output.
* `data_check --traceback` - print traceback output for debugging.
* `data_check --version` - Show the version and exit.
* `data_check --help` - show this help message and exit.

## Exit codes

Possible exit codes:

__Exit code 0:__ All tests run successfully.<br/>
__Exit code 1:__ At least one test failed.


## Loading data into tables

Sometimes you need to populate tables with some data before running pipeline tests. With data_check you can use CSV file to load data into the tables. The [CSV format](usage.md#csv-format) is the same as used for testing. The header in the CSV file must match the columns in the table. If the table doesn't exists, it will be created.

The schema and table names are always case-insensitive, as long as the database supports it. Otherwise they are lowercased.

### Loading data into a single table

To load data from some CSV file into a table, you can use `data_check --load path/to/csv_file.csv --table schema.table_name`. This will truncate the table and load the content of the CSV file into the table. You can specify different [load modes](usage.md#load-modes) if you do not want to truncate the table.

### Loading data into multiple tables

You can use multiple CSV files or a whole folder to load the data into tables. The table name will be derived from the file name.

* `data_check --load-tables path/schema.table_1.csv` - this will load the data from the CSV file into the table schema.table_1
* `data_check --load-tables path/to/some_folder` - this will load the data from all CSV files in this folder into tables matching the file names.

The path will be searched recursively for CSV files. The folder structure doesn't matter when matching the table names, only the file name matters.

### Load modes

There are multiple modes that control the data loading:

* truncate - truncate the table before loading the data.
* replace - drop the table and recreate it based on the columns in the CSV file.
* append - do not touch the table before loading the data.

By default, the tables will be truncated before loading the data. You can use the other modes with `--load-mode`:

`data_check --load file.csv --table table_name --load-mode replace`
`data_check --load-tables path/to/tables_folder --load-mode append`

### CSV and data types

When loading data from CSV files, data_check (or more precisely: [pandas](https://pandas.pydata.org/)) will infer the data types from the file.
When loading the data into the table, the database will usually implicitly convert the data types.
This works good for simple data types like strings and numbers.

If you need to load date types (or timestamps) and the table has a date column, data_check will infer the date format from the CSV file for that column.
This doesn't work when using `--load-mode replace` since the table will be dropped before it can be analyzed. This will probably result in a varchar column instead of date.

It's best to use [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) for dates.

## Executing arbitrary SQL code

You can run any SQL file against the database by using the `--run-sql` command:

`data_check --run-sql sql_file.sql other_file.sql`

or a whole folder recursively:

`data_check --run-sql some/folder/with/sql_files`

All files are run in parallel. If you have dependencies between the files, data_check must be called sequentially for each file.

Multiple statements in a SQL file usually must be inside an anonymous block. MySQL doesn't support this however.
