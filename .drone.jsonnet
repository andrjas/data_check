# run `./mkdrone.sh` whenever you change this file to generate .drone.yml and .drone.arm64.yml

local arch = std.extVar("arch");

local generic_int_test = [
    "poetry run data_check ping --wait --timeout 60 --retry 5",
    "rm -f checks/generated/data_with_hash.csv",
    "rm -f test.db",
    "poetry run data_check sql --workers 1 --files prepare",
    "poetry run pytest ../test_int_tests.py",
    "poetry run pytest ../../../test/database",
];

local generic_prepare_commands(db) =
[
    "python -m pip install -U pip",
    "python -m pip install poetry",
    "cp -rn example/checks test/int_test/"+db,
    "cp -rn example/load_data test/int_test/"+db,
    "cp -rn example/run_sql test/int_test/"+db,
    "cp -rn example/lookups test/int_test/"+db,
    "cp -rn example/fake test/int_test/"+db,
    "cd test/int_test/"+db,
]+
if db == "sqlite" then ["poetry install"] else ["poetry install -E "+db]
;


local int_pipeline(db, image, prepare_commands=[], environment={}, db_image="", service_extra={}, extra_volumes=[], image_extra_volumes=[], pkg_cache_path="/var/cache/apt") =
{
    kind: "pipeline",
    type: "docker",
    name: db,
    platform: {
        "os": "linux",
        "arch": arch
    },
    steps: [
        {
            name: "data_check",
            image: image,
            pull: "if-not-exists",
            commands: prepare_commands + generic_prepare_commands(db) + generic_int_test,
            environment: environment,
            volumes: [
                {
                    name: "cache",
                    path: "/root/.cache"
                },
                {
                    name: "pkg_cache",
                    path: pkg_cache_path
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
                path: "/tmp/data_check_cache/"+db
            }
        },
        {
            name: "pkg_cache",
            host: {
                path: "/tmp/data_check_cache/"+db+"_pkg"
            }
        }
    ] + extra_volumes
};


local sqlite_test() = int_pipeline("sqlite", "python:3.9");


local postgres_test() = int_pipeline("postgres", "python:3.9", [],
{
    DB_USER: {from_secret: "POSTGRES_USER"},
    DB_PASSWORD: {from_secret: "POSTGRES_PASSWORD"},
    DB_CONNECTION: {from_secret: "POSTGRES_CONNECTION"},
});


local mysql_test() = int_pipeline("mysql", "python:3.9", [],
{
    DB_USER: {from_secret: "MYSQL_USER"},
    DB_PASSWORD: {from_secret: "MYSQL_PASSWORD"},
    DB_CONNECTION: {from_secret: "MYSQL_CONNECTION"},
});


local mssql_test() = int_pipeline("mssql", "python:3.9",
[
    "curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -",
    "curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list",
    "rm /etc/apt/apt.conf.d/docker-clean",  # enable caching
    "apt-get update",
    "apt-get install -y unixodbc unixodbc-dev python3.9-dev",
    "ACCEPT_EULA=Y apt-get install -y msodbcsql18",
],
{
    DB_USER: {from_secret: "MSSQL_USER"},
    DB_PASSWORD: {from_secret: "MSSQL_PASSWORD"},
    DB_CONNECTION: {from_secret: "MSSQL_CONNECTION"},
});


local oracle_test() = int_pipeline("oracledb", "python:3.9", [],
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
], "/var/cache/dnf"
);


[
oracle_test(),
sqlite_test(),
mysql_test(),
postgres_test(),
mssql_test(),
]
