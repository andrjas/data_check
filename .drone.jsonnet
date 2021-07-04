local python_test(version) = 
{
    "kind": "pipeline",
    "type": "docker",
    "name": "python_"+version,
    "steps": [
        {
            "name": "python",
            "image": "python:"+version,
            "commands": [
                "python3 -m pip install -U pip",
                "python3 -m pip install poetry",
                "poetry install",
                "poetry run pytest test"
            ]
        }
    ]
};


local generic_int_test = [
    "bash -c 'while ! poetry run data_check --ping; do echo \"waiting for db\"; sleep 1; done'",
    "poetry run data_check --run-sql prepare",
    "poetry run pytest ../../test/test_database.py ../../test/test_data_check.py",
    "poetry run data_check --generate checks/generated",
    "poetry run data_check checks/basic checks/generated --traceback",
    "bash -c 'if ! poetry run data_check checks/failing; then exit 0; else exit 1; fi'",
    "poetry run data_check checks/pipelines/simple_pipeline --traceback"
];


local sqllite_int_test = [
    "bash -c 'while ! poetry run data_check --ping; do echo \"waiting for db\"; sleep 1; done'",
    "poetry run pytest test/test_database.py test/test_data_check.py",
    "poetry run data_check --generate checks/generated",
    "poetry run data_check checks/basic checks/generated --traceback",
    "bash -c 'if ! poetry run data_check checks/failing; then exit 0; else exit 1; fi'",
    "poetry run data_check checks/pipelines/simple_pipeline --traceback --workers 1"
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


local sqlite_test() = int_pipeline("sqlite", "python:3.8",
[
    "python -m pip install -U pip",
    "python -m pip install poetry",
    "poetry install"
], int_test=sqllite_int_test);


local postgres_test() = int_pipeline("postgres", "python:3.8",
[
    "python -m pip install -U pip",
    "python -m pip install poetry",
    "poetry install -E postgres",
    "cd int_test/postgres"
],
{
    POSTGRES_PASSWORD: "data_check"
}, "postgres:13");


local mysql_test() = int_pipeline("mysql", "python:3.8",
[
    "python -m pip install -U pip",
    "python -m pip install poetry",
    "poetry install -E mysql",
    "cd int_test/mysql"
],
{
    MYSQL_ROOT_PASSWORD: "data_check"
}, "mysql:8");


local mssql_test() = int_pipeline("mssql", "python:3.8",
[
    "curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -",
    "curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list",
    "apt-get update",
    "apt-get install -y unixodbc unixodbc-dev",
    "ACCEPT_EULA=Y apt-get install -y msodbcsql17",
    "python -m pip install -U pip",
    "python -m pip install poetry",
    "poetry install -E mssql",
    "cd int_test/mssql"
],
{
    ACCEPT_EULA: "Y",
    MSSQL_PID: "Express",
    SA_PASSWORD: "data_CHECK",
}, "mcr.microsoft.com/mssql/server:2019-latest");


local oracle_test() = int_pipeline("oracle", "centos:7",
[
    "yum install -y wget  ncurses libnsl",
    "yum-config-manager --add-repo https://yum.oracle.com/public-yum-ol7.repo",
    "wget http://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle",
    "yum install -y oracle-release-el7",
    "mv /etc/yum.repos.d/oracle-ol7.repo.incomplete /etc/yum.repos.d/oracle-ol7.repo",
    "yum install -y oracle-instantclient19.5-basic oracle-instantclient19.5-sqlplus python3",
    "python3 -m pip install -U pip",
    # centos 7 needs an older cryptography version
    "python3 -m pip install poetry cryptography==3.3.2",
    "poetry install -E oracle",
    "cd int_test/oracle"
],
{
    ORACLE_PWD: "data_check",
    CLIENT_HOME: "/usr/lib/oracle/19.5/client64",
    LD_LIBRARY_PATH: "${LD_LIBRARY_PATH}:${CLIENT_HOME}/lib",
    PATH: "${PATH}:${CLIENT_HOME}/bin:/usr/local/bin",
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
python_test("3.6.2"),
python_test("3.7"),
python_test("3.8"),
python_test("3.9"),
sqlite_test(),
postgres_test(),
mysql_test(),
mssql_test(),
oracle_test()
]
