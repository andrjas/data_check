# Changelog

## [Unreleased]

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
