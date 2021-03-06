#!/bin/bash

set -e

# 

if [ "$(basename "${PWD}")" == 'mysql' ]; then
    # run this script from the main path
    cd ../.. || exit 1
fi

dc="int_test/mysql/docker-compose.yml"

# tear down old tests
# As MySQL doesn't support anonymous code blocks,
# we need to tear down the db to start each test from scratch.
docker-compose -f "$dc" rm -s -f -v data_check db

docker-compose -f "$dc" build --no-rm
docker-compose -f "$dc" up -d db

docker-compose -f "$dc" up --exit-code-from data_check data_check
