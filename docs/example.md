# Examples

## data_check sample project

This Git repository is also a sample data_check project. Clone the repository, switch to the folder and run data_check:

```
git clone git@github.com:andrjas/data_check.git
cd data_check
data_check
```

This will run the tests in the _checks_ folder using the default connection as defined in data_check.yml.

The result will tell you which tests passed and which failed:

```
checks/generated/generate_before_running.sql: NO EXPECTED RESULTS FILE
checks/failing/invalid.sql: FAILED (with exception in checks/failing/invalid.sql)
checks/failing/expected_to_fail.sql: FAILED
checks/basic/simple_string.sql: PASSED
checks/basic/data_types.sql: PASSED
checks/basic/float.sql: PASSED
checks/basic/unicode_string.sql: PASSED
```

## Tests structure

You can structure your test in many ways. You can also mix there structures.

### By pipeline

You can structure your tests to run before/after some data pipeline has run:

```
checks/
    pipeline1/
        pre/
            test1.sql
            test1.csv
            ...
        post/
            ...
    pipeline2/
        pre/
            ...
        post/
            ...
```

### By test execution time

In a CI environment you can structure your tests after the expected execution time of the tests.

```
checks/
    quick_tests/
        ...
    medium_tests/
        ...
    slow_running_tests/
        ...
```

This way you can run quick test, e.g. schema validation, many times during development. Other tests that must process a lot of data can be run less frequently, e.g. in a integration environment.

## Database URLs

These are the connection strings used in the [integration test](development.md#integration-tests). Other connection strings can be found in the [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/13/core/engines.html).

### PostgreSQL

postgresql://username:password@db_host:5432/db_name

### Oracle

oracle+cx_oracle://username:password@db_host:1521/?service_name=XEPDB1

### MySQL/MariaDB

mysql+pymysql://username:password@db_host:3306/db_name

### Microsoft SQL Server

mssql+pyodbc://username:password@db_host:1433/db_name?driver=ODBC+Driver+17+for+SQL+Server
