#!/bin/bash

# 

if [ "$(basename "${PWD}")" == 'postgres' ]; then
    # run this script from the main path
    cd ../.. || exit 1
fi

dc="int_test/postgres/docker-compose.yml"

# tear down old tests
docker-compose -f "$dc" down -v

docker-compose -f "$dc" build --no-rm
docker-compose -f "$dc" up -d db

docker-compose -f "$dc" up --exit-code-from data_check data_check
