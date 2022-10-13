# CSV checks

This is the default mode when running data_check. data_check expects a [SQL file](#sql-file) and a [CSV file](#csv-format). The SQL file will be executed against the database and the result is compared with the CSV file. If they match, the test is passed, otherwise it fails.

## SQL file

Each SQL file must contain a single SQL query. The query will be executed in the database, hence you must use the SQL dialect that the database in use understands.

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

data_check uses a basic CSV format. Each column is separated by a comma without any space around them.
The first line must contain a header. The columns must match the columns in the SQL file.

Any columns that do not match between the CSV and the SQL file will be ignored.

```csv
string_header,int_header,float_header,date_header,null_header
string,42,42.1,2020-12-20,
# a comment line
"second row",42,42.1,2020-12-20,
```

Anything after '#' is regarded as a comment. You can use comments to annotate the date as they will be completely ignored.
You can escape the start of the comment with '\' and threat the rest of the line as data.

Only the data types strings, decimals and date/timestamps (partially) are supported. Strings can be optionally enclosed in double quotes (").
Empty strings are treated as null values.

data_check recognizes if a column in a SQL query is a date or timestamp and converts the columns in the CSV automatically to timestamps.
For some databases (PostgreSQL, MySQL) this only works for timestamp/datetime columns, not date columns.
The date format in the CSV file must use [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), for example "2021-12-30" or "2021-12-30 13:45:56".

Any other data type must be converted to strings/varchar in the SQL query.


## Empty dataset checks

If you expect the result of the SQL query to be empty, you do not have to write a CSV file with the header. Instead, create a file with the ending _.empty_.
Since the column names do not matter, the check will pass, if the SQL query does not return any values.

The _.empty_ file can be empty or contain any data. data_check does not read the content from this file.

Example:

```
empty_query.sql
empty_query.empty
```


## Full table checks

To compare the content of a whole table, you can also put a CSV or Excel file without the SQL file. The file must be named after the table name. data_check will only compare the columns in the file. If the table does not have a column from the CSV/Excel header, the test will fail.

Example:

_schema.table\_name.csv_:
```csv
column1,column2
data1,data2
...
```

This will run this SQL query: `select column1, column2 from schema.table_name` and compare the result against the CSV file.

## Generating expectation files

If you run `data_check gen` in a project folder, data_check will execute the query for each SQL file where the CSV file is missing and write the results into the CSV file. You can add `--force` to overwrite existing CSV files.

You can also generate expectation files for [pipelines](pipelines.md#generating-pipeline.checks). If you run `data_check gen` on a project with pipelines, beware though that the pipelines will be executed!

## Lookups

Lookups are a way to reuse the result of a query in multiple other queries. You can use lookups for example to return all table names that you want to exclude from your tests and use the table names to filter them from your queries.

To create a lookup, put a SQL file in the _lookups_ folder. To use the lookup in a query, use the lookup name like a subquery or list. The lookup name is the SQL filename prefixed with a colon (':') and without the file ending. If a lookup file is in subfolders, each subfolder is part of the lookup name with a double underscore ('__') as the separator between folders and the SQL filename.

Only the first column from a lookup query is used, other columns are ignored.

Example:

_some\_check.sql_:
```sql
select a
from some_table
where b in :b1
 or a in :sub_lkp__b2
```

_lookups/b1.sql_:
```sql
select 'b' as b
union all
select 'c' as b

```

_lookups/sub\_lkp/b2.sql_:
```sql
select 1 as b
union all
select 2 as b
```

In this example `:b1` is loaded from the file _lookups/b1.sql_ and `:sub_lkp__b2` from _lookups/sub\_lkp/b2.sql_.

## Data types

data_check tries its best to match the data types from the SQL result with the data types from the CSV file.
There are, however, some edge cases where data_check has to convert the data types based on a heuristic:

If the SQL result or the CSV file have float64 values in a column, the column of the other part will also be converted to float64,
since float64 might be represented in scientific notation. If the conversion fails, the check will fail anyways.

If the SQL result or the CSV file have mixed string/numeric values in a column, data_check will convert the column to float64 if the other column has only float64 values. Otherwise it will convert both columns to string values.

In all other cases, if the matching columns have different data types, data_check will convert them to the string representation.

For date types, data_check is using internally the python datetime type. pandas Timestamp cannot be used here, since it has a lower [limit](https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.max.html) for dates than all the supported databases.
