#!/bin/bash

# run all int tests and print an error if any of them failed

result=0

cd "$(dirname "$0")" || exit 1

failed_tests=""

for test in mssql mysql oracle postgres;
do
    cd "$test" || exit 1
    if ! ./int_test.sh; then
        result=1
        failed_tests="${failed_tests} $test"
    fi
    cd .. || exit 1
done

echo "######################"
if [ $result != 0 ]; then
    echo "integration tests failed: ${failed_tests}"
else
    echo "integration tests succeeded"
fi

exit $result
