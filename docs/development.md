# Development

Use the [Dev Container](https://containers.dev/) to create a dev environment.

Alternatively, install [uv](https://docs.astral.sh/uv/) and run `uv sync --frozen`.

## Testing

data_check has two layers of tests:

### Unit tests

[pytest](https://pytest.org/) is used for unit testing. There are two types of tests for data_check in the _test_ folder: Basic tests for the code and tests against a database.

For unit tests an in-memory SQLite database that is integrated into Python is used.

Run `pytest` inside the virtual environment to execute the unit tests.

### Integration tests

Integration tests are using specific databases and run unit tests and data_check test against this database.

These databases used for integration tests:

- PostgreSQL
- MySQL
- Oracle
- Microsoft SQL Server
- DuckDB
- SQLite

The integration tests are run with GitHub Actions or locally with the script `scripts/run_int_test.sh`.

## Python support

The GitHub Action in `.github/workflows/ci-pr.yml` is used to test the support for different python versions.
