# Pipelines

If data_check finds a file named _data\_check\_pipeline.yml_ in a folder, it will treat this folder as a pipeline check. Instead of running [CSV checks](csv_checks.md) it will execute the steps in the YAML file.

## Example

Example project with a pipeline:

    data_check.yml
    checks/
        some_test.sql                # this test will run in parallel to the pipeline test
        some_test.csv
        sample_pipeline/
            data_check_pipeline.yml  # configuration for the pipeline
            data/
                my_schema.some_table.csv       # data for a table
            data2/
                some_data.csv        # other data
            some_checks/             # folder with CSV checks
                check1.sql
                check1.csl
                ...
            run_this.sql             # a SQL file that will be executed
            cleanup.sql
        other_pipeline/              # you can have multiple pipelines that will run in parallel
            data_check_pipeline.yml
            ...

The file _sample\_pipeline/data\_check\_pipeline.yml_ can look like this:

```yaml
steps:
    # this will truncate the table my_schema.some_table and load it with the data from data/my_schema.some_table.csv
    - load: data
    # this will execute the SQL statement in run_this.sql
    - sql: run_this.sql
    # this will append the data from data2/some_data.csv to my_schema.other_table
    - load:
        file: data2/some_data.csv
        table: my_schema.other_table
        mode: append
    # this will run a python script and pass the connection name
    - cmd: "python3 /path/to/my_pipeline.py --connection {{CONNECTION}}"
    # this will run the CSV checks in the some_checks folder
    - check: some_checks
    - always_run:
        - sql: run_this_always.sql
```

Pipeline checks and simple CSV checks can coexist in a project.

## Pipeline configuration

_data\_check\_pipeline.yml_ is a YAML file with _steps_ as its main element that contains a list of steps.


## steps

_steps_ is a list of steps that are executed in the pipeline sequentially. If any of the steps fail the pipeline will fail and the following steps will not be executed.

Most steps have a list of files as a parameter. If you only need a single file/path, you can usually use a short form: `step_name: path`.

### check

_check_ will run [CSV checks](csv_checks.md) in a given folder or from a single file. It is like running data_check with this parameter. All checks will be performed in parallel.

Short form:
```yaml
- check: some_checks
```

Long form:
```yaml
- check:
    files:
    - some_checks
    - some/other/checks.sql
```

You can also omit _files_:
```yaml
- check:
    - some_checks
    - some/other/checks.sql
```

_run_ is an alias for check:
```yaml
- run: some_checks
```

You can omit _mode_. Then the default mode _truncate_ will be used. _load\_mode_ can be used instead of _mode_, but it is deprecated.

### load

_load_ is like calling `data_check load ...`. This will load one or more tables from CSV files and infer the table name from the file name.
You can also override the table name when using a single CSV file.

Like with `data_check load` the path before the filename has no impact on the inferred table name, only the file name itself.

Short form:
```yaml
- load: some_path/schema.table_name.csv
```

Long form:
```yaml
- load:
    files:
      - some_path
      - some/other/path/schema.other_table.csv
    mode: append
```

You can omit _mode_. Then the default mode _truncate_ will be used. _load\_mode_ can be used instead of _mode_, but it is deprecated.

You can also omit _files_:
```yaml
- load:
    - some_path
    - some/other/path/schema.other_table.csv
```

Or use _file_ as an alias for _files:
```yaml
- load:
    file: some_file.csv
```

Overriding the table name:
```yaml
- load:
    table: some_table
    file: some_file.csv
```

You can only use a single CSV file when overriding the table name. Using multiple files will fail.

### append

_append_ is an alias for

```yaml
- load:
    mode: append
```


Short form:
```yaml
- append: some_path/schema.table_name.csv
```

Long form:
```yaml
- append:
    files:
      - some_path
      - some/other/path/schema.other_table.csv
```


### sql

_sql_ is like calling `data_check sql ...`.
_sql_ has two modes: query mode and files mode.
In the short form, _sql_ will check if the given parameter is a file and run the file in files mode.
If the parameter is not a file, _sql_ will execute in query mode.

Query mode and files mode can be explicitly specified by using the long form of _sql_.

#### query mode

The query mode is like calling `data_check sql ...` without `--files`. This will execute a SQL statement given as the parameter. If the SQL is a query, the result will be printed as CSV. If the parameter is a file path, the

Short form:
```yaml
- sql: select 1 as a, 'b' as t
```

Long form:
```yaml
- sql:
    query: select 1 as a, 'b' as t
```

With _output_ to write a CSV file:
```yaml
- sql:
    query: select 1 as a, 'b' as t
    output: result.csv
```

_output_ is relative to the pipeline path, unless an absolute path is specified, for example '{{PROJECT_PATH}}/result.csv'.
_output_ can only be used with a query, not with files.

_write\_check_ can be used to generate a CSV check from a SQL statement:
```yaml
- sql:
    query: select 1 as a, 'b' as t
    write_check: some_check.sql
```

See [here](sql.md#generating-a-csv-check-from-a-sql-statement) for more information.

#### files mode

The query mode is like calling `data_check sql --files ...`. This will run a SQL file or all SQL files in a folder against the configured database. All SQL files are executed in parallel. If you need to execute a file after another file, you need to call _sql_ twice.

Short form:
```yaml
- sql: some_file.sql
```

Long form:
```yaml
- sql:
    files:
      - some_file.sql
      - some_path
```

You can also omit _files_:
```yaml
- sql:
    - some_file.sql
    - some_path
```


### cmd

_cmd_ will call any script or program. The commands will be executed sequentially. The optional _print_ parameter can disable console output of the command.

Short form:
```yaml
- cmd: echo "test"
```

Long form:
```yaml
- cmd:
  commands:
    - echo "test"
    - script/to/start/pipeline.sh
  print: false
```

With `print: false` no output is printed from the commands.

You can also omit _commands_:
```yaml
- cmd:
  - echo "test"
  - script/to/start/pipeline.sh
```

### always_run

_always\_run_ is a container for other steps. These steps will always be executed, even if any other step fail. If _always\_run_ is between other steps, it will be executed in order.

Example:
```yaml
steps:
  - sql: might_fail.sql
  - always_run:
    - sql: run_after_failing.sql
    - cmd: some_script.sh
  - cmd: other_script.sh
  - always_run:
    - sql: finish.sql
```

In this example, if _might\_fail.sql_ fails, _run\_after\_failing.sql_, _some\_script.sh_ and _finish.sql_ will be run in this order. If _might\_fail.sql_ does not fail, _other\_script.sh_ is executed after _run\_after\_failing.sql_ and _some\_script.sh_. _finish.sql_ will then run at the end (even when _other\_script.sh_ fails).

### fake

_fake_ is used to generate test data. See [here](test_data.md) for more details and how to create the configuration.

Short form:
```yaml
- fake: fake_conf.yml
```

Long form:
```yaml
- fake:
    configs:
        - fake_conf.yml
        - fake_conf2.yml
```

With custom output path:
```yaml
- fake:
    configs:
        - fake_conf.yml
    output: fake_table.csv
```

### ping
_ping_ is like calling `data_check ping --wait`. This will try to connect to the database and fail the pipeline if it doesn't succeed within the timeout. Timeout duration and time between retries can be configured.

Short form:
```yaml
- ping:
```

With timeout and retry configuration:
```yaml
- ping:
    timeout: 5
    retry: 1
```
This will retry the connection each second and timeout after 5 seconds, which are the default values.

### sql_files

_sql\_files_ is deprecated. Use [sql](#sql) instead.

_sql\_files_ is like calling `data_check sql --files ...`. This will run a SQL file or all SQL files in a folder against the configured database. All SQL files are executed in parallel. If you need to execute a file after another file, you need to call _sql\_files_ twice. _sql\_file_ is an alias for _sql\_files_.

Short form:
```yaml
- sql_files: some_file.sql
```

Using the alias:
```yaml
- sql_file: some_file.sql
```

Long form:
```yaml
- sql_files:
    files:
      - some_file.sql
      - some_path
```

You can also omit _files_:
```yaml
- sql_files:
    - some_file.sql
    - some_path
```

### load_table

_load\_table_ is deprecated. Use [load](#load) instead.

_load\_table_ is like calling `data_check load --table ...`. This will load a CSV file into a table.

```yaml
- load_table:
    file: check/date_test.csv
    table: temp.date_test
    mode: append
```

## nested pipelines

Pipelines can be nested inside other pipelines. Passing a folder with a _data\_check\_pipeline.yml_ to _check_ will run the pipeline:

```yaml
- check:
    - some_checks
    - folder_with_a_pipeline
```

## Parameters in pipelines

You can use some predefined parameters in a pipeline definition:

* CONNECTION: The name of the connection used for this run.
* CONNECTION_STRING: The connection string as defined in _data\_check.yml_ used for the connection.
* PROJECT_PATH: The path of the data_check project (the folder containing _data\_check.yml_).
* PIPELINE_PATH: The absolute path to the pipeline (the folder containing _data\_check\_pipeline.yml_).
* PIPELINE_NAME: The name of the folder containing _data\_check\_pipeline.yml_.


## Generating pipeline checks

Like [generating expectation files](csv_checks.md#generating-expectation-files) you can also run `data_check gen` for a pipeline. In this mode the pipeline is executed, but each _check_ step will generate the CSV files instead of running the actual checks. Adding `--force` will overwrite existing CSV files.


## Debugging a pipeline

You can add a breakpoint between pipeline steps to stop the pipeline and run the Python Debugger (pdb).

Example:
```yaml
steps:
  - sql: select 1 as a, 'b' as t {{from_dual}}
  - breakpoint:
  - sql: select 2 as a, 'c' as t {{from_dual}}
```

You have access to all [debugger commands](https://docs.python.org/3/library/pdb.html#debugger-commands), e.g. 'c', 'cont' or 'continue' to continue execution until the next breakpoint. 'q' or 'quit' will quit the debugger and the pipeline will fail.

Additionally you can use these variables and function to interact with the pipeline:

* data_check: access to the main data_check instance
* sql: access to SQL functions
* pipeline: access to the current pipeline
* steps: list of all steps in the pipeline
* current_step(): returns the current step
* next_step(): returns the next step without executing it
* run_next(): run the next step in the pipeline

To run a SQL statement in the debugger, use `sql.run_sql("<any sql statement>")`. If you want to use the result as a DataFrame, use `sql.run_query("select ...")` which will return a DataFrame.
