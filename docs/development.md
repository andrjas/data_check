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

There are currently four databases used for integration tests:

- PostgreSQL
- MySQL
- Oracle
- Microsoft SQL Server

The integration tests are run via [Drone CI](https://www.drone.io/). The file _.drone.yml_ is generated from _.drone.jsonnet_ and checked in into the Git repository. To update _.drone.yml_ run `drone jsonnet  --format --stream`.

To speed up integration tests the CI pipeline uses local, pre-build docker images. These images are maintained in a separate [repository](https://github.com/andrjas/data_check_images).

## Python support

[tox](https://github.com/tox-dev/tox) is used to test data_check against multiple python versions.

Multiple python versions can be intalled with [pyenv](https://github.com/pyenv/pyenv):

```
pyenv install 3.8.1
pyenv install 3.9.1
pyenv install 3.10.1

pyenv local 3.8.1 3.9.1 3.10.1
```

Install all versions mentioned in _tox.ini_. Then run the tests with tox: `tox`.
