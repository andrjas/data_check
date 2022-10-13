# run "drone jsonnet  --format --stream" whenever you change this file
# to generate .drone.yml

local generic_int_test = [
    "bash -c 'while ! poetry run data_check ping --quiet; do sleep 1; done'",
    "poetry run pytest ../test_int_tests.py",
    "poetry run pytest ../../../test/database",
];


local int_pipeline(db, image, prepare_commands, environment={}, db_image="", service_extra={}, extra_volumes=[], image_extra_volumes=[]) =
{
    kind: "pipeline",
    type: "docker",
    name: db,
    steps: [
        {
            name: "data_check",
            image: image,
            pull: "if-not-exists",
            commands: prepare_commands + generic_int_test,
            environment: environment,
            volumes: [
                {
                    name: "cache",
                    path: "/root/.cache"
                }
            ] + image_extra_volumes
        }
    ],
    [if db_image != "" then "services"]: [
        {
            "name": "db",
            "image": db_image,
            "environment": environment
        } + service_extra
    ],
    volumes: [
        {
            name: "cache",
            host: {
                path: "/tmp/data_check_cache"
            }
        }
    ] + extra_volumes
};


local sqlite_test() = int_pipeline("sqlite", "local/poetry:3.8",
[
    "cp -rn example/checks test/int_test/sqlite",
    "cp -rn example/load_data test/int_test/sqlite",
    "cp -rn example/run_sql test/int_test/sqlite",
    "cp -rn example/lookups test/int_test/sqlite",
    "cp -rn example/fake test/int_test/sqlite",
    "poetry install",
    "cd test/int_test/sqlite"
]);


local postgres_test() = int_pipeline("postgres", "local/poetry:3.8",
[
    "cp -rn example/checks test/int_test/postgres",
    "cp -rn example/load_data test/int_test/postgres",
    "cp -rn example/run_sql test/int_test/postgres",
    "cp -rn example/lookups test/int_test/postgres",
    "cp -rn example/fake test/int_test/postgres",
    "poetry install -E postgres",
    "cd test/int_test/postgres"
],
{
    DB_USER: {from_secret: "POSTGRES_USER"},
    DB_PASSWORD: {from_secret: "POSTGRES_PASSWORD"},
    DB_CONNECTION: {from_secret: "POSTGRES_CONNECTION"},
});


local mysql_test() = int_pipeline("mysql", "local/poetry:3.8",
[
    "cp -rn example/checks test/int_test/mysql",
    "cp -rn example/load_data test/int_test/mysql",
    "cp -rn example/run_sql test/int_test/mysql",
    "cp -rn example/lookups test/int_test/mysql",
    "cp -rn example/fake test/int_test/mysql",
    "poetry install -E mysql",
    "cd test/int_test/mysql"
],
{
    DB_USER: {from_secret: "MYSQL_USER"},
    DB_PASSWORD: {from_secret: "MYSQL_PASSWORD"},
    DB_CONNECTION: {from_secret: "MYSQL_CONNECTION"},
});


local mssql_test() = int_pipeline("mssql", "local/poetry_mssql",
[
    "cp -rn example/checks test/int_test/mssql",
    "cp -rn example/load_data test/int_test/mssql",
    "cp -rn example/run_sql test/int_test/mssql",
    "cp -rn example/lookups test/int_test/mssql",
    "cp -rn example/fake test/int_test/mssql",
    "poetry install -E mssql",
    "cd test/int_test/mssql"
],
{
    DB_USER: {from_secret: "MSSQL_USER"},
    DB_PASSWORD: {from_secret: "MSSQL_PASSWORD"},
    DB_CONNECTION: {from_secret: "MSSQL_CONNECTION"},
});


local oracle_test() = int_pipeline("oracle", "local/poetry_oracle",
[
    "cp -rn example/checks test/int_test/oracle",
    "cp -rn example/load_data test/int_test/oracle",
    "cp -rn example/run_sql test/int_test/oracle",
    "cp -rn example/lookups test/int_test/oracle",
    "cp -rn example/fake test/int_test/oracle",
    "poetry install -E oracle",
    "cd test/int_test/oracle"
],
{
    DB_USER: {from_secret: "ORACLE_USER"},
    DB_PASSWORD: {from_secret: "ORACLE_PASSWORD"},
    DB_CONNECTION: {from_secret: "ORACLE_CONNECTION"},
    NLS_LANG: ".utf8",
    LC_ALL: "en_US.utf-8",
    LANG: "en_US.utf-8",
    TNS_ADMIN: "/app/network/admin"
}, "", {}, [
    {
        name: "wallet",
        host: {
            path: "/home/data_check/wallet"
        }
    }
], [
    {
        name: "wallet",
        path: "/app/network/admin"
    }
]);


[
oracle_test(),
sqlite_test(),
mysql_test(),
postgres_test(),
mssql_test(),
]
