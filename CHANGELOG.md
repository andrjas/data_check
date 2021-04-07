# Changelog

## [Unreleased]

### Added

- --print-format to output failed data in csv instead of pandas format
- dont start executor pool, when a single file or --workers=1
- CLI tests
- --force to overwrite files when generating

### Fixed

- stop immediately when using unknown connection

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
