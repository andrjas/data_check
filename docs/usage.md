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
* `data_check -g/--gen/--generate` - generate expectation files if they don't exist.
* `data_check --force` - when set, --generate will overwrite files.
* `data_check --config CONFIG` - config file to use (default: data_check.yml).
* `data_check --load PATH --table`-  load table data from a csv into the table
* `data_check --load-method METHOD` -  how to load the table: truncate (default), append or replace. Use with --load or --load-tables.
* `data_check --load-tables some_folder/or/some_file.csv`-  load tables from a list of csv files
* `data_check --run-sql some_folder/or/some_file.sql` - run any SQL script in a list of SQL files
* `data_check --ping` - tries to connect to the database.
* `data_check --verbose` - print verbose output.
* `data_check --traceback` - print traceback output for debugging.
* `data_check --version` - Show the version and exit.
* `data_check --help` - show this help message and exit.

## Exit codes

Possible exit codes:

__Exit code 0:__ All tests run successfully.<br/>
__Exit code 1:__ At least one test failed.

## SQL file

Each SQL file must contain a single SQL query. The query be run against the database, hence you must use the SQL dialect that the database in use understands.

### Templates

SQL files can contain Jinja2 templates. The templates are replaced with the values defined in _checks/template.yml_. Example:

SQL file:
```sql
select '{{template_value}}' as test
```

_checks/template.yml_:
```yaml
template_value: ABC
```

CSV file:

```csv
test
ABC
```


## CSV format

data_check uses a pretty basic CSV format. Each column is separated by a comma without any space around them.
The first line must contain a header. The columns must match the columns in the SQL file. 

Any columns that do not match between the CSV and the SQL file will be ignored.

```csv
string_header,int_header,float_header,date_header,null_header
string,42,42.1,2020-12-20,
# a comment line
"second row",42,42.1,2020-12-20,
```

Each file starting with a '#' is regarded as a comment. You can use comments to annotate the date as they will be completely ignored.

Only the data types strings and decimals are supported. Strings can be optionally enclosed in double quotes (").
Empty strings are treated as null values.

Any other data type (e.g. date) must be converted to strings/varchar in the SQL query.

## Generating expectation files

If you run `data_check --generate` in a project folder, data_check will execute the query for each SQL file where the CSV file is missing and write the results into the CSV file.

## Loading data into tables

Sometimes you need to populate tables with some data before running pipeline tests. With data_check you can use CSV file to load data into the tables. The [CSV format](usage.md#csv-format) is the same as used for testing. The header in the CSV file must match the columns in the table. If the table doesn't exists, it will be created.

The schema and table names are always case-insensitive, as long as the database supports it. Otherwise they are lowercased.

### Loading data into a single table

To load data from some CSV file into a table, you can use `data_check --load path/to/csv_file.csv --table schema.table_name`. This will truncate the table and load the content of the CSV file into the table. You can specify different [load methods](usage.md#load-methods) if you do not want to truncate the table.

### Loading data into multiple tables

You can use multiple CSV files or a whole folder to load the data into tables. The table name will be derived from the file name.

* `data_check --load-tables path/schema.table_1.csv` - this will load the data from the CSV file into the table schema.table_1
* `data_check --load-tables path/to/some_folder` - this will load the data from all CSV files in this folder into tables matching the file names.

The path will be searched recursively for CSV files. The folder structure doesn't matter when matching the table names, only the file name matters.

### Load methods

There are multiple methods that control the data loading:

* truncate - truncate the table before loading the data.
* replace - drop the table and recreate it based on the columns in the CSV file.
* append - do not touch the table before loading the data.

By default, the tables will be truncated before loading the data. You can use the other methods with `--load-method`:

`data_check --load file.csv --table table_name --load-method replace`
`data_check --load-tables path/to/tables_folder --load-method append`

## Executing arbitrary SQL code

You can run any SQL file against the database by using the `--run-sql` command:

`data_check --run-sql sql_file.sql other_file.sql`

or a whole folder recursively:

`data_check --run-sql some/folder/with/sql_files`

All files are run in parallel. If you have dependencies between the files, data_check must be called sequentially for each file.

Multiple statements in a SQL file usually must be inside an anonymous block. MySQL doesn't support this however.
