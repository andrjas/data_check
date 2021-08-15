# CSV checks

This is the default mode when running data_check. data_check expects a [SQL file](#sql-file) and a [CSV file](#csv-format). The SQL file will be executed against the database and the result is compared with the CSV file. If they match, the test is passed, otherwise it fails.

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

Only the data types strings, decimals and date/timestamps (partially) are supported. Strings can be optionally enclosed in double quotes (").
Empty strings are treated as null values.

data_check recognizes if a column in a SQL query is a date or timestamp and converts the columns in the CSV automatically to timestamps.
For some databases (PostgreSQL, MySQL) this only works for timestamp columns, not date columns.
The date format in the CSV file is inferred, but it's best to use [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601).

Any other data type must be converted to strings/varchar in the SQL query.

## Large date values

data_check uses primarly pandas Timestamp to store date values. However, there is a [limit](https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.max.html) how large the date can be (currently 2262-04-11 23:47:16.854775807). If you need to process larger date values (especially 9999-12-31 to represent "infinity" in historized tables), data_check needs some input to process it right. Otherwise the column in the SQL query will have the value [NaT](https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html) but the result from the CSV file will have the date represented as a string and the test will fail.

The SQL query can have a date hint to tell data_check to parse this column as a [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601):

```sql
-- date: some_date_column, other_date_column
select ...
```

This will tell data_check to parse the columns _some\_date\_column_ and _other\_date\_column_ as dates in the SQL query as well as in the CSV file.


## Generating expectation files

If you run `data_check --generate` in a project folder, data_check will execute the query for each SQL file where the CSV file is missing and write the results into the CSV file. You can add `--force` to overwrite existing CSV files.

You can also generate expectation files for [pipelines](pipelines.md#generating-pipeline.checks). If you run `--generate` on a project with pipelines, beware though that the pipelines will be executed!
