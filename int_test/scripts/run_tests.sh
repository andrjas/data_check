#!/bin/bash

DB="$1"

# wait for DB to start
while ! data_check --config "int_test/${DB}/data_check.yml" --ping; do
    echo "waiting for db"
    sleep 1
done

result=0

# run basic unit tests first
if ! pytest test; then
    result=1
fi

# then the integration tests
cd "int_test/${DB}" || exit 1
if ! pytest ../../test; then
    result=1
fi

# and start data_check
if ! data_check --gen; then
    result=1
fi

echo "testing posivite run"
if ! data_check checks/basic checks/generated; then
    result=1
fi

echo "testing negative run"
if data_check checks/failing; then
    result=1
fi

echo "######################"
if [ $result != 0 ]; then
    echo "integration tests for ${DB} failed!"
else
    echo "integration tests for ${DB} succeeded"
fi

exit $result
