# Changelog

## [Unreleased]

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
