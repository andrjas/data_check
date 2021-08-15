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
    - load_tables: data
    # this will execute the SQL statement in run_this.sql
    - sql_file: run_this.sql
    # this will append the data from data2/some_data.csv to my_schema.other_table
    - load:
        file: data2/some_data.csv
        table: my_schema.other_table
        load_mode: append
    # this will run a python script and pass the connection name
    - cmd: "python3 /path/to/my_pipeline.py --connection {{CONNECTION}}"
    # this will run the CSV checks in the some_checks folder
    - check: some_checks
```

Pipeline checks and simple CSV checks can coexist in a project.

## Pipeline configuration

_data\_check\_pipeline.yml_ is a YAML file with _steps_ as its main element that contains a list of steps.


## Steps

_steps_ is a list of steps that are executed in the pipeline sequentially. If any of the steps fail the pipeline will fail and the following steps will not be executed.

Most steps have a list of files as a parameter. You you only need a single file/path, you can usually use a short form: `step_name: path`.

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


### load

_load_ is like calling `data_check --load ...`. This will load a CSV file into a table.

```yaml
- load:
    file: check/date_test.csv
    table: temp.date_test
    load_mode: append
```

You can omit _load\_mode_. Then the default mode _truncate_ will be used.

### load_tables

_load\_tables_ is like calling `data_check --load-tables ...`. This will load one or more tables from CSV files and infer the table name from the file name.

Like with `--load-tables` the path before the filename has no impact on the inferred table name, only the file name itself.

Short form:
```yaml
- load_tables: some_path/schema.table_name.csv
```

Long form:
```yaml
- load_tables:
    files: 
      - some_path
      - some/other/path/schema.other_table.csv
    load_mode: append
```

You can omit _load\_mode_. Then the default mode _truncate_ will be used.

You can also omit _files_:
```yaml
- load_tables:
    - some_path
    - some/other/path/schema.other_table.csv
```

### sql_files

_sql\_files_ is like calling `data_check --sql-files ...`. This will run a SQL file or all SQL files in a folder againts the configured database. All SQL files are executed in parallel. If you need to execute a file after another file, you need to call _sql\_files_ twice. _sql\_file_ is an alias for _sql\_files_.

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

### sql

_sql_ is like calling `data_check --sql ...`. This will execute a SQL statement given as the parameter. If the SQL is a query, the result will be printed as CSV.

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


### cmd

_cmd_ will call any script or program. The commands will be executed sequentially.

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
```

You can also omit _commands_:
```yaml
- cmd:
  - echo "test"
  - script/to/start/pipeline.sh
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
* PROJECT_PATH: The path of the data_check project, i.e. the folder containing _data\_check.yml_.
* PIPELINE_PATH: The path to the pipeline, i.e. the folder containing _data\_check\_pipeline.yml_.


## Generating pipeline checks

Like [generating expectation files](csv_checks.md#generating-expectation-files) you can also run `data_check --generate` for a pipeline. In this mode the pipeline is executed, but each _check_ step will generate the CSV files instead of running the actual checks. Adding `--force` will overwrite existing CSV files.
