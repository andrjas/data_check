# Installing data_check

Depending on your system there are many ways to install data_check. Generally, the steps are always the same:

- create a virtual environment
- activate the virtual environment
- optionally: install pip
- install data_check

You should then be able to run `data_check`.

To run data_check next time in a new terminal you must just activate the virtual environment.

Note: While the tool is called 'data_check' the package that you install is called 'data-check'.

## With pipx

The easiest way to install data_check is to use [pipx](https://github.com/pipxproject/pipx):

`pipx install data-check`

To upgrade data_check via pipx: `pipx upgrade data-check`

## With Anaconda/Miniconda

Create a new conda environment:

`conda create --name data_check python=3.11`

Activate the environment:

`conda activate data_check`

and install pip:

`conda install pip`

Install data_check via pip:

`python3 -m pip install data-check`

To upgrade data_check in the conda environment: `python3 -m pip install data-check --upgrade`

### With venv

Create a virtual environment (`<venv>` point to a relative or absolute path, e.g. c:\venvs\data_check)

`python3 -m venv <venv>`

Activate the environment:

Bash: `source <venv>/bin/activate`
PowerShell: `<venv>\Scripts\Activate.ps1`

The [Python documentation](https://docs.python.org/3/library/venv.html) has more options how to enable a virtual environment.

Install data_check via pip:

`python3 -m pip install data-check`

To upgrade data_check in the virtual environment: `python3 -m pip install data-check --upgrade`

## Databases

Installing data_check alone will only support SQLite, which is bundled with Python. You need additional drivers for other databases. See [https://docs.sqlalchemy.org/en/20/dialects/index.html](https://docs.sqlalchemy.org/en/20/dialects/index.html) for all possible drivers and how to install them.

With pipx you can install the drivers with `pipx inject data-check <drivername>` (watch the minus sign in _data-check_ instead of the underscore).
In a virtual environment you just activate the environment and run `pip install <drivername>`.

Some drivers need additional dependencies. Here are the drivers used for [testing](development.md#testing) data_check:

### PostgreSQL

`psycopg2-binary` should work on most systems without any additional dependencies.

You can use `data-check[postgres]` to install data_check directly with `psycopg2-binary`:
e.g. with pipx: `pipx install data-check[postgres]`

### MySQL/MariaDB

`PyMySQL` as described in [https://pypi.org/project/PyMySQL/](https://pypi.org/project/PyMySQL/) with additional cryptography dependencies.

Use `pipx install data-check[mysql]` to install data_check with `PyMySQL[rsa]`.

### Microsoft SQL Server

`pyodbc` needs unixodbc and the development package (unixodbc-dev) on Linux.

Additionally you must install the
[Microsoft ODBC driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server) on your system.

Use `pipx install data-check[mssql]` to install data_check with `pyodbc`.

### Oracle

`cx_Oracle` needs Oracle client libraries to work. [https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) shows how to install them.

Use `pipx install data-check[oracle]` to install data_check with `cx_Oracle`.

Alternatively you can use [python-oracledb](https://oracle.github.io/python-oracledb/) that does not requires any extra libraries.

Use `pipx install data-check[oracledb]` to install data_check with `python-oracledb`.
