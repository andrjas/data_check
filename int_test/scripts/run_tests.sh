#!/bin/bash

DB="$1"

# wait for DB to start
while ! poetry run data_check --config "int_test/${DB}/data_check.yml" --ping; do
    echo "waiting for db"
    sleep 1
done

result=0

cd "int_test/${DB}" || exit 1
if ! poetry run data_check --run-sql prepare; then
    result=1
fi

if ! poetry run pytest ../../test/test_database.py ../../test/test_data_check.py; then
    result=1
fi

# and start data_check
if ! poetry run data_check --gen; then
    result=1
fi

echo "testing posivite run"
if ! poetry run data_check checks/basic checks/generated --traceback; then
    result=1
fi

echo "testing negative run"
if poetry run data_check checks/failing; then
    result=1
fi

echo "######################"
if [ $result != 0 ]; then
    echo "integration tests for ${DB} failed!"
else
    echo "integration tests for ${DB} succeeded"
fi

exit $result
