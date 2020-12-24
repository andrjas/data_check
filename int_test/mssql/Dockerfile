FROM python:3.8

# This Dockerfile creates an image with mssql client and data_check

# dependencies for pyodbc and https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    apt-get install -y unixodbc unixodbc-dev && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

RUN useradd -m data_check && mkdir /app && chown data_check /app
USER data_check

WORKDIR /app
ENV PATH="/home/data_check/.local/bin/:${PATH}"

COPY --chown=data_check requirements.txt .
COPY --chown=data_check dependencies/requirements-dev.txt .
COPY --chown=data_check dependencies/requirements-mssql.txt .


RUN python3 -m pip install --user --no-cache-dir -r requirements.txt && \
    python3 -m pip install --user --no-cache-dir -r requirements-dev.txt && \
    python3 -m pip install --user --no-cache-dir -r requirements-mssql.txt

COPY --chown=data_check . .
COPY --chown=data_check int_test/scripts .

RUN python3 -m pip install --user -e .

CMD ["sh", "-c", "/app/run_tests.sh mssql"]
