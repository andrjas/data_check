#!/bin/bash

# run all int tests and print an error if any of them failed

result=0

cd "$(dirname "$0")/../.." || exit 1

failed_tests=""

for version in 3.6 3.7 3.8 3.9;
do
    if ! docker build -f "int_test/python/${version}/Dockerfile" -t "data_check_py${version}" .; then
        result=1
        failed_tests="${failed_tests} ${version}"
    else
        if ! docker run --rm "data_check_py${version}"; then
            result=1
            failed_tests="${failed_tests} ${version}"
        fi
    fi
    docker rmi --force "data_check_py${version}"
done

echo "######################"
if [ $result != 0 ]; then
    echo "python tests failed: ${failed_tests}"
else
    echo "python tests succeeded"
fi

exit $result
