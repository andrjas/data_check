# Changelog

## [Unreleased]

### Changed
- updated dependencies
- simplified dependency update process

### Removed
- Python 3.8 support

## [0.17.0] - 2023-07-18

## Added
- pipeline YAML validation via pydantic
- more breakpoint step features and documentation

### Changed
- replaced 'overall result' with 'summary'


### Fixed
- load_template and load_lookups called twice in run
- generating sorted csv for checks
- updated SQLAlchemy links to 2.0
- print exception if merging non-unique columns

## [0.16.0] - 2023-06-09

### Added
- CI with ARM64 MSSQL driver
- oracledb as alternative for cx_oracle
- `--use-process` parameter to switch back to ProcessPoolExecutor

### Changed
- upgraded to pandas 2
- upgraded to SQLAlchemy 2
- switched to ThreadPoolExecutor by default


## [0.15.0] - 2023-02-17

### Added
-- 'data_check init' to create projects and pipelines
-- 'append' as alias for append-mode in cli and pipelines
-- 'ping --wait' and --timeout/--retry
-- Python 3.11 support

### Changed
-- io module is renamed to file_ops
-- running csv file without matching sql file will fail, otherwise it will run the csv check
-- MSSQL uses arm64 image for CI

### Fixed
-- NA/NaT should be treated equally in checks
-- CTRL+C should work in Windows
-- 'data_check gen' works with full table checks

### Removed
- custom docker images for CI

## [0.14.0] - 2023-01-13

### Added
- pre-commit hooks with various tools for code quality
- project wide default_load_mode configuration
- pipelines: added 'files' for 'sql' to deprecate 'sql_files'
- pipelines: added 'run' as alias for 'check'
- tests that pipeline steps matches cli
- pipelines: 'write_check' for 'sql'
- documentation for 'fake' pipeline step
- pipelines: added 'table' and 'file' for 'load' to deprecate 'load_table'
- running data_check_pipeline.yml directly to execute the pipeline


### Changed
- refactored TableInfo into Table
- moved integration tests into pytest
- upgraded dependencies

### Fixed
- load fails if csv doesn't have all columns

### Deprecated
- pipelines: 'sql_files' is deprecated, use 'sql' instead
- pipelines: 'load_table' is deprecated, use 'load' instead

## [0.13.0] - 2022-09-29

### Added
- upsert mode for loading data into tables
- pipelines: added 'mode' to deprecate 'load_mode'
- env variable DATA_CHECK_CONNECTION can override default connection

### Changed
- printing exception on failure without --traceback
- upgraded dependencies
- documentation theme

### Fixed
- Oracle: using VARCHAR2 instead of CLOB to load strings and large decimals
- bug in runner.executor when calculating max_workers

### Deprecated
- pipelines: 'load_mode' is deprecated, use 'mode' instead

### Removed
- workaround for replace mode
- support for python 3.7
- importlib-metadata dependency

## [0.12.0] - 2022-04-13

### Added
- test data generator with Faker

### Changed
- CLI uses subcommands
- load and load_table in pipeline YAML
- CI uses DB connections via secrets

### Fixed
- loading mixed date/null values

## [0.11.1] - 2022-02-16

### Fixed
- SettingWithCopyWarning in failing checks
## [0.11.0] - 2022-02-15

### Added
- `--sql` and `--sql-files` use lookups
- full table checks
- `--print --diff` to print only changed columns
- `--write-check` to generate a CSV check

### Changed
- example project moved into subfolder
- split main into cli module
- rewrote cli testing using click.testing.CliRunner
- `--sql` with `--output` doesn't print on console

### Fixed
- recursive process spawning
- pipeline does not stop on error
- log file is written into project path
- `--print` with empty set prints result when failing

## [0.10.0] - 2021-12-27

### Added

- python 3.10 support
- tox to test multiple python versions

### Removed

- python 3.6 support
- python tests in int_test

### Fixed

- --sql --output produces empty lines and \r\r\n on windows
- --generate produces non-UTF8 files


## [0.9.0] - 2021-12-17

### Added

- lookups
- always_run steps in pipelines
- print parameter in cmd pipeline step
- logging in config file and --log argument
- tests that --sql and sql in a pipeline use templates
- test for non-ASCII characters in column names
- '\' as escape character in CSVs
- --print/--print-json with --verbose prints the output even if it's matching


### Removed

- --gen argument (use -g or --generate instead)

### Fixed

- handling pd.NaT
- --generate escapes '#' in CSV
- --ping works with --verbose and --traceback

## [0.8.0] - 2021-10-29

### Added

- Excel support (for checks and loading tables)
- --quiet suppresses all output
- simpler empty dataset checks
- testing if all commands are documented
- tests for nullable date columns
- tests for large dates (9999-12-31)

### Changed

- date handling: columns from the database and CSV files are better recognized as dates
- tests are split into multiple folders
- cli tests are moved to integration tests
- refactoring: DataCheck class no longer inherits the check classes

### Removed

- date hints

### Fixed

- checks now fail if an invalid path is given


## [0.7.0] - 2021-08-16

### Added

- SQLite int tests
- --sql parses templates in statement
- can start data_check in a subfolder of a project
- print sql statement in pipeline
- pipeline --generate mode

### Changed

- refactored int tests using templates and pre-built docker images
- renamed --run-sql/run_sql to --sql-files/sql_files
- marking tests as failed if result length differs
- --print outputs CSV format
- --print output is sorted

### Removed

- --print-csv (as this is what --print does now)


## [0.6.0] - 2021-08-04

### Added

- --sql parameter to run SQL statements directly from command line and print the result as CSV
- sql pipeline step
- --output/-o parameter to write --sql generated CSV file
- --run-sql prints result as CSV if it is a query
- --print --format json and --print-json
- date hints
- support for large dates (e.g. 9999-12-31 00:00:00)

### Changed

- CSV checks now convert date columns automatically
- collect_data returns ordered list, i.e. serial runs are deterministically
- using truncate table instead of delete where possible

### Fixed

- run_sql fails if running in parallel with multiple files in a folder
- CSV handling numbers with leading zeros as strings

## [0.5.0] - 2021-07-28

### Added

- loading date/timestamp from CSV files
- pipelines

### Changed

- using Drone CI for integration tests
- renamed --load-method to --load-mode

### Removed

- old integration test scripts

## [0.4.0] - 2021-07-01

### Added

- loading tables from CSV files
- running any SQL file
- more tests
- integration tests for all supported python versions

### Changed

- upgraded dependencies
- minimum python version is now 3.6.2
- internal refactoring

### Removed

- unit tests inside integration tests

## [0.3.0] - 2021-04-09

### Added

- --print-format to output failed data in csv instead of pandas format
- dont start executor pool, when a single file or --workers=1
- CLI tests
- --force to overwrite files when generating
- --print-csv as shortcut for "--print --print-format csv"
- environment variables support for connection strings
- simple jinja templating for SQL queries

### Changed

- upgraded dependencies
- upgraded SQLAlchemy to 1.4

### Fixed

- stop immediately when using unknown connection
- CLI tests for python 3.6

## [0.2.1] - 2021-02-28

- colored output on PowerShell

## [0.2.0] - 2021-02-23

### Added

- --verbose argument
- --traceback argument
- -g argument as an alias for --generate
- colored output
- overall result output
- teardown script for integration tests

### Changed

- using poetry for packaging and integration tests
- failing integration test when any command fails

### Fixed

- SettingWithCopyWarning from pandas when using --print with a failing test

## [0.1.1] - 2021-02-07

### Fixed

- using ProcessPoolExecutor for parallel queries
- merge fails if a varchar column only has ints/decimals in result
- SAWarning for Oracle

## [0.1.0] - 2020-12-25

- initial release
