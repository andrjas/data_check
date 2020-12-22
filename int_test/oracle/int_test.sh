#!/bin/bash

# You need Oracle 18c XE to run this integration test. This requires some manual steps:
# - download oracle-database-xe-18c-1.0-1.x86_64.rpm from https://www.oracle.com/database/technologies/xe-downloads.html
# - git clone https://github.com/oracle/docker-images.git
# - copy oracle-database-xe-18c-1.0-1.x86_64.rpm to docker-images/OracleDatabase/SingleInstance/dockerfiles/18.4.0
# - cd docker-images/OracleDatabase/SingleInstance/dockerfiles
# - ./buildDockerImage.sh -v 18.4.0 -x


if [ "$(basename "${PWD}")" == 'oracle' ]; then
    # run this script from the main path
    cd ../.. || exit 1
fi

dc="int_test/oracle/docker-compose.yml"

# tear down old tests
# keep oracle container, as it will take time to startup
docker-compose -f "$dc" rm -s -f -v data_check

docker-compose -f "$dc" build --no-rm
docker-compose -f "$dc" up -d db

docker-compose -f "$dc" up --exit-code-from data_check data_check
