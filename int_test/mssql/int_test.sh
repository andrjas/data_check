#!/bin/bash

# 

if [ "$(basename "${PWD}")" == 'mssql' ]; then
    # run this script from the main path
    cd ../.. || exit 1
fi

dc="int_test/mssql/docker-compose.yml"

# tear down old tests
docker-compose -f "$dc" rm -s -f data_check

docker-compose -f "$dc" build --no-rm
docker-compose -f "$dc" up -d db

docker-compose -f "$dc" up --exit-code-from data_check data_check
