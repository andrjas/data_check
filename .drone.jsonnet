# run "drone jsonnet  --format --stream" whenever you change this file
# to generate .drone.yml

local positive_int_tests = [
    "checks/basic",
    "checks/generated",
    "checks/empty_sets/basic",
    "checks/excel/basic",
    "checks/pipelines/simple_pipeline",
    "checks/pipelines/date_test",
    "checks/pipelines/leading_zeros",
];

local failing_int_tests = [
    "checks/failing/duplicates.sql",
    "checks/failing/expected_to_fail.sql",
    "checks/failing/invalid_csv.sql",
    "checks/failing/invalid.sql",
    "checks/empty_sets/failing/not_empty_query.sql",
    "checks/excel/failing/failing_empty.sql",
    "checks/excel/failing/failing_excel.sql",
];

local generic_int_test = [
    "bash -c 'while ! poetry run data_check --ping --quiet; do sleep 1; done'",
    "poetry run data_check --sql-files prepare",
    "poetry run pytest ../../../test/database",
    "poetry run data_check --generate checks/generated",
] + [
    "poetry run data_check %s --traceback --print" % [p] for p in positive_int_tests
] + [
    "bash -c 'if ! poetry run data_check %s; then exit 0; else exit 1; fi'" % [f] for f in failing_int_tests
];


local int_pipeline(db, image, prepare_commands, environment={}, db_image="", service_extra={}, pipeline_extra={}, int_test=generic_int_test) = 
{
    kind: "pipeline",
    type: "docker",
    name: db,
    steps: [
        {
            name: "data_check",
            image: image,
            pull: "if-not-exists",
            commands: prepare_commands + int_test,
            environment: environment
        }
    ],
    [if db_image != "" then "services"]: [
        {
            "name": "db",
            "image": db_image,
            "environment": environment
        } + service_extra
    ],
} + pipeline_extra;


local sqlite_test() = int_pipeline("sqlite", "local/poetry:3.8",
[
    "cp -rn example/checks test/int_test/sqlite",
    "cp -rn example/load_data test/int_test/sqlite",
    "cp -rn example/run_sql test/int_test/sqlite",
    "cp -rn example/lookups test/int_test/sqlite",
    "poetry install",
    "cd test/int_test/sqlite"
]);


local postgres_test() = int_pipeline("postgres", "local/poetry:3.8",
[
    "cp -rn example/checks test/int_test/postgres",
    "cp -rn example/load_data test/int_test/postgres",
    "cp -rn example/run_sql test/int_test/postgres",
    "cp -rn example/lookups test/int_test/postgres",
    "poetry install -E postgres",
    "cd test/int_test/postgres"
],
{
    POSTGRES_PASSWORD: "data_check"
}, "postgres:13");


local mysql_test() = int_pipeline("mysql", "local/poetry:3.8",
[
    "cp -rn example/checks test/int_test/mysql",
    "cp -rn example/load_data test/int_test/mysql",
    "cp -rn example/run_sql test/int_test/mysql",
    "cp -rn example/lookups test/int_test/mysql",
    "poetry install -E mysql",
    "cd test/int_test/mysql"
],
{
    MYSQL_ROOT_PASSWORD: "data_check"
}, "mysql:8");


local mssql_test() = int_pipeline("mssql", "local/poetry_mssql",
[
    "cp -rn example/checks test/int_test/mssql",
    "cp -rn example/load_data test/int_test/mssql",
    "cp -rn example/run_sql test/int_test/mssql",
    "cp -rn example/lookups test/int_test/mssql",
    "poetry install -E mssql",
    "cd test/int_test/mssql"
],
{
    ACCEPT_EULA: "Y",
    MSSQL_PID: "Express",
    SA_PASSWORD: "data_CHECK",
}, "mcr.microsoft.com/mssql/server:2019-latest");


local oracle_test() = int_pipeline("oracle", "local/poetry_oracle",
[
    "cp -rn example/checks test/int_test/oracle",
    "cp -rn example/load_data test/int_test/oracle",
    "cp -rn example/run_sql test/int_test/oracle",
    "cp -rn example/lookups test/int_test/oracle",
    "poetry install -E oracle",
    "cd test/int_test/oracle"
],
{
    ORACLE_PWD: "data_check",
    NLS_LANG: ".utf8",
    LC_ALL: "en_US.utf-8",
    LANG: "en_US.utf-8"

}, "oracle/database:18.4.0-xe", 
service_extra={
    settings: {
        shm_size: '1gb',
    },
    volumes: [{
        name: "oradata",
        path: "/opt/oracle/oradata"  
    }]
},
pipeline_extra={
    volumes: [{
        name: "oradata",
        host: {
            path: "/var/lib/oradata"
        } 
    }]
});


[
oracle_test(),
sqlite_test(),
mysql_test(),
postgres_test(),
mssql_test(),
]
