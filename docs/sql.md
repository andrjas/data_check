# Executing arbitrary SQL code

You can run any SQL file against the database by using the `sql --files` command:

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

## Generating a CSV check from a SQL statement

You can use `sql` to create a CSV check:

`data_check sql "select * from tableX" --write-check some_check.sql`

This writes the SQL statement into `some_check.sql` and the result as CSV into `some_check.csv`.
