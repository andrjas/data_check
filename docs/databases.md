# Databases

These databases are supported by data_check. The instructions assume you use `pipx`. If you want to install the database drivers inside a virtual environment, replace `pipx` with `pip`.

Note: Do not store the credentials, especially the password directly, in the configuration! Use environment variables instead.
For example `postgresql://${DB_USER}:${DB_PASSWORD}@${DB_CONNECTION}` instead of `postgresql://username:password@db_host:5432/db_name`.

## PostgreSQL

### Installation

Use `data-check[postgres]` to install data_check with PostgreSQL support:

```
pipx install data-check[postgres]
```

This will install `psycopg2-binary` as the database driver. `psycopg2-binary` should work on most systems without any additional dependencies.

### Connection string

```
postgresql://username:password@db_host:5432/db_name
```


## MySQL/MariaDB

### Installation

Use `data-check[mysql]` to install data_check with MySQL/MariaDB support:

```
pipx install data-check[mysql]
```

This will install `PyMySQL[rsa]` as described in [https://pypi.org/project/PyMySQL/](https://pypi.org/project/PyMySQL/) with additional cryptography dependencies.

### Connection string

```
mysql+pymysql://username:password@db_host:3306/db_name
```


## Microsoft SQL Server

### Installation

Use `data-check[mssql]` to install data_check with Microsoft SQL Server support:

```
pipx install data-check[mssql]
```

This will install `pyodbc` which needs `unixodbc` and the development package (`unixodbc-dev`) on Linux.

Additionally you must install the [Microsoft ODBC driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server) on your system.

### Connection string

```
mssql+pyodbc://username:password@db_host:1433/db_name?driver=ODBC+Driver+18+for+SQL+Server
```


## Oracle

You can choose between `oracledb` and `cx_Oracle` for Oracle. `oracledb` is preferred.

### oracledb

#### Installation

Use `data-check[oracledb]` to install data_check with Oracle support:

```
pipx install data-check[oracledb]
```

This will install [python-oracledb](https://oracle.github.io/python-oracledb/) which does not requires any extra libraries.

#### Connection string

```
oracle+oracledb://username:password@db_host:1521/?service_name=XEPDB1
```


### cx_Oracle

#### Installation

Use `data-check[oracle]` to install data_check with Oracle support:

```
pipx install data-check[oracle]
```

`cx_Oracle` needs Oracle client libraries. See [https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) how to install them.

### Connection string

```
oracle+cx_oracle://username:password@db_host:1521/?service_name=XEPDB1
```


## DuckDB

### Installation

Use `data-check[duckdb]` to install data_check with DuckDB support:

```
pipx install data-check[duckdb]
```

This will install [duckdb-engine](https://github.com/Mause/duckdb_engine).

### Connection string

```
duckdb:///path/to/duck.db
```

Note: This will use a relative path to the database file. Use `duckdb:////full/path/to/duck.db` to specify a full path.

### Limitations

The [load modes](loading_data.md#load-modes) `replace` and `upsert` will not work with DuckDB since `duckdb-engine` does not support reflection on indices yet.


## Databricks

### Installation

Use `data-check[databricks]` to install data_check with Databricks support:

```
pipx install data-check[databricks]
```

This will install [databricks-sql-python](https://github.com/databricks/databricks-sql-python) with the `sqlalchemy` option.

### Connection string

```
databricks://token:${access_token}@${host}?http_path=${http_path}&catalog=${catalog}&schema=${schema}
```

### Limitations

[Lookups](csv_checks.md#lookups) do not work yet.

The upsert [load mode](loading_data.md#load-modes) is not tested for Databricks.
