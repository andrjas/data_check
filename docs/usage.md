# Usage

## Commands

* `data_check` - Run data_check agains the default connection in the _checks_ folder.
* `data_check some_folder` - Run data_check agains the default connection in the _some_folder_ folder.
* `data_check some_folder/some_file.sql` - Run data_check against the default connection for a single test.
* `data_check -h/--help` - show this help message and exit.
* `data_check -c/--connection CONNECTION` - use another connection than the default.
* `data_check -n/--workers WORKERS` - use WORKERS threads to run the queries (default: 4).
* `data_check --print` - print failed results data.
* `data_check -g/--gen/--generate` - generate expectation files if they don't exist.
* `data_check --config CONFIG` - config file to use (default: data_check.yml).
* `data_check --ping` - tries to connect to the database.
* `data_check --version` - print version and exit.
* `data_check --verbose` - print verbose output.
* `data_check --traceback` - print traceback output for debugging.

## Exit codes

Possible exit codes:

__Exit code 0:__ All tests run successfully.<br/>
__Exit code 1:__ At least one test failed.

## SQL file

Each SQL file must contain a single SQL query. The query be run against the database, hence you must use the SQL dialect that the database in use understands.

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
