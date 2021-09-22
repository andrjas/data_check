# run "drone jsonnet  --format --stream" whenever you change this file
# to generate .drone.yml

local python_test(version) = 
{
    "kind": "pipeline",
    "type": "docker",
    "name": "python_"+version,
    "steps": [
        {
            "name": "python",
            "image": "local/poetry:"+version,
            "commands": [
                "poetry install",
                "poetry run pytest test int_test/cli",
                "cd checks/basic",
                "poetry run data_check",
            ]
        }
    ]
};


local generic_int_test = [
    "bash -c 'while ! poetry run data_check --ping --quiet; do sleep 1; done'",
    "poetry run data_check --sql-files prepare",
    "poetry run pytest ../../test/database",
    "poetry run data_check --generate checks/generated",
    "poetry run data_check checks/basic checks/generated checks/empty_sets/basic --traceback",
    "bash -c 'if ! poetry run data_check checks/failing; then exit 0; else exit 1; fi'",
    "bash -c 'if ! poetry run data_check checks/empty_sets/failing; then exit 0; else exit 1; fi'",
    "poetry run data_check checks/pipelines/simple_pipeline --traceback",
    "poetry run data_check checks/pipelines/date_test --traceback --print",
    "poetry run data_check checks/pipelines/leading_zeros --traceback --print",
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
    "cp -rn checks int_test/sqlite",
    "cp -rn load_data int_test/sqlite",
    "cp -rn run_sql int_test/sqlite",
    "poetry install",
    "cd int_test/sqlite"
]);


local postgres_test() = int_pipeline("postgres", "local/poetry:3.8",
[
    "cp -rn checks int_test/postgres",
    "cp -rn load_data int_test/postgres",
    "cp -rn run_sql int_test/postgres",
    "poetry install -E postgres",
    "cd int_test/postgres"
],
{
    POSTGRES_PASSWORD: "data_check"
}, "postgres:13");


local mysql_test() = int_pipeline("mysql", "local/poetry:3.8",
[
    "cp -rn checks int_test/mysql",
    "cp -rn load_data int_test/mysql",
    "cp -rn run_sql int_test/mysql",
    "poetry install -E mysql",
    "cd int_test/mysql"
],
{
    MYSQL_ROOT_PASSWORD: "data_check"
}, "mysql:8");


local mssql_test() = int_pipeline("mssql", "local/poetry_mssql",
[
    "cp -rn checks int_test/mssql",
    "cp -rn load_data int_test/mssql",
    "cp -rn run_sql int_test/mssql",
    "poetry install -E mssql",
    "cd int_test/mssql"
],
{
    ACCEPT_EULA: "Y",
    MSSQL_PID: "Express",
    SA_PASSWORD: "data_CHECK",
}, "mcr.microsoft.com/mssql/server:2019-latest");


local oracle_test() = int_pipeline("oracle", "local/poetry_oracle",
[
    "cp -rn checks int_test/oracle",
    "cp -rn load_data int_test/oracle",
    "cp -rn run_sql int_test/oracle",
    "poetry install -E oracle",
    "cd int_test/oracle"
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
python_test("3.6.2"),
sqlite_test(),
python_test("3.7"),
mysql_test(),
python_test("3.8"),
postgres_test(),
python_test("3.9"),
mssql_test(),
]
