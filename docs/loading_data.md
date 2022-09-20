# Loading data into tables

Sometimes you need to populate tables with some data before running pipeline tests. With data_check you can use CSV or Excel files to load data into tables. The [CSV format](usage.md#csv-format) is the same as used for testing. The header in the CSV file or the first row in the Excel file must match the columns in the table. Additionally, Excel cells can use its native date/datetime format.

If the table doesn't exist, it will be created. The schema and table names are always case-insensitive, as long as the database supports it. Otherwise, they are lowercased.

## Loading data into a single table

To load data from some CSV or Excel file into a table, you can use `data_check load path/to/some_file.[csv/xlsx] --table schema.table_name`. This will truncate the table and load the content of the file into the table. You can specify different [load modes](usage.md#load-modes) if you do not want to truncate the table.

## Loading data into multiple tables

You can use multiple CSV and Excel files or a whole folder to load the data into tables. The table name will be derived from the file name.

* `data_check load path/schema.table_1.csv` - this will load the data from the CSV file into the table schema.table_1
* `data_check load path/schema.table_2.xlsx` - this will load the data from the Excel file into the table schema.table_2
* `data_check load path/to/some_folder` - this will load the data from all CSV and Excel files in this folder into tables matching the file names.

The path will be searched recursively for CSV and Excel files. The folder structure doesn't matter when matching the table names, only the file name matters.

## Load modes

There are multiple modes that control the data loading:

* truncate - truncate the table before loading the data.
* replace - drop the table and recreate it based on the columns in the CSV or Excel file.
* append - do not touch the table before loading the data.

By default, the tables will be truncated before loading the data. You can use the other modes with `--mode`:

`data_check load file.csv --table table_name --mode replace`
`data_check load path/to/tables_folder --mode append`

## CSV and data types

When loading data from CSV files, data_check (or more precisely: [pandas](https://pandas.pydata.org/)) will infer the data types from the file.
When loading the data into the table, the database will usually implicitly convert the data types.
This works good for simple data types like strings and numbers.

If you need to load date types (or timestamps) and the table has a date column, data_check will try to convert these columns in the CSV file into a datetime.
This doesn't work when using `--mode replace` since the table will be dropped before it can be analyzed. This will probably result in a varchar column instead of date.

Use [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) for dates, like for [CSV checks]((csv_checks.md#csv-format)
