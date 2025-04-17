# Development

[poetry](https://python-poetry.org/) must be installed first for development.

To set up a development environment initially with poetry:

```bash
poetry install
```

Later, just activate the virtual environment: `poetry shell`

Please use [Black](https://github.com/psf/black) to format the code before committing any change: `black data_check`

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

[tox](https://github.com/tox-dev/tox) is used to test data_check against multiple python versions.

Multiple python versions can be installed with [pyenv](https://github.com/pyenv/pyenv):

```
pyenv install 3.9.1
pyenv install 3.10.1
pyenv install 3.11.1
pyenv install 3.12.1

pyenv local 3.9.1 3.10.1 3.11.1 3.12.1
```

Install all versions mentioned in _tox.ini_. Then run the tests with tox: `tox`.
