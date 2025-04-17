#!/bin/bash

# Runs the integration tests of data_check.
# Expected environment variables:
# - DB_CONNECTION
# - DB_PASSWORD
# - DB_USER

set -e

db_type=$1

if [[ "$db_type" == "sqlite" ]]; then
    uv sync
else
    uv sync --extra $db_type
fi

if [[ "$db_type" == "mssql" ]]; then
    if ! [[ "20.04 22.04 24.04 24.10" == *"$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)"* ]];
    then
        echo "Ubuntu $(grep VERSION_ID /etc/os-release | cut -d '"' -f 2) is not currently supported.";
        exit;
    fi

    # Download the package to configure the Microsoft repo
    curl -sSL -O https://packages.microsoft.com/config/ubuntu/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)/packages-microsoft-prod.deb
    # Install the package
    sudo dpkg -i packages-microsoft-prod.deb
    # Delete the file
    rm packages-microsoft-prod.deb

    # Install the driver
    apt-get update
    ACCEPT_EULA=Y apt-get install -y msodbcsql18
elif [[ "$db_type" = "oracledb" ]]; then
    export NLS_LANG=".utf8"
    export LC_ALL="en_US.utf-8"
    export LANG="en_US.utf-8"
    export TNS_ADMIN="/app/network/admin"
fi

cp -r --update=none example/checks test/int_test/$db_type
cp -r --update=none example/load_data test/int_test/$db_type
cp -r --update=none example/run_sql test/int_test/$db_type
cp -r --update=none example/lookups test/int_test/$db_type
cp -r --update=none example/fake test/int_test/$db_type

cd test/int_test/$db_type

uv run data_check ping --wait --timeout 60 --retry 5
rm -f checks/generated/data_with_hash.csv
rm -f test.db

uv run data_check sql --workers 1 --files prepare
uv run pytest ../test_int_tests.py
uv run pytest ../../../test/database
