#!/bin/bash

# shuts down all running docker containers from the integration test

cd "$(dirname "$0")" || exit 1

for test in mssql mysql oracle postgres;
do
    echo "teardown $test"
    docker-compose -f "$test/docker-compose.yml" down -v
done
