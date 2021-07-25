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

Only the data types strings and decimals are supported. Strings can be optionally enclosed in double quotes (").
Empty strings are treated as null values.

Any other data type (e.g. date) must be converted to strings/varchar in the SQL query.

## Generating expectation files

If you run `data_check --generate` in a project folder, data_check will execute the query for each SQL file where the CSV file is missing and write the results into the CSV file.