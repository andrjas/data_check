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

[pytest](https://pytest.org/) is used for unit testing. There are two types of test for data_check in the _test_ folder: Basic tests for the code and tests against a database.

For unit tests an in-memory SQLite database that is integrated into Python is used.

Run `pytest` inside the virtual environment to execute the unit tests.

### Integration tests

Integration tests are using specific databases and run unit tests and data_check test against this database.

There are currently four databases used for integration tests:

- PostgreSQL
- MySQL
- Oracle
- Microsoft SQL Server

All integration tests are located in the _int_test_ folder.

The tests should be executed from the root folder of this Git repository. Run `./int_test/<db>/int_test.sh` to execute a test against a database, e.g. `./int_test/postgres/int_test.sh`

The scripts are using Docker and Docker Compose to set up a Docker container with the database and another with data_check and all required database drivers.

First the same unit tests as mentioned above are run against the database. Then data_check is executed. Each database has its own _checks_ folder in _int_test_ to adhere to different SQL dialects.
