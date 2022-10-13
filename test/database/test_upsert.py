import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import Column, Integer, MetaData, String, Table

from data_check.sql import DataCheckSql
from data_check.sql.load_mode import LoadMode


def create_test_table(table_name: str, schema: str, sql: DataCheckSql):
    sql.table_loader.drop_table_if_exists(table_name, schema)
    if sql.dialect == "oracle":
        sql.run_sql(
            f"create table {schema}.{table_name} (id number(10) primary key, data varchar2(10))"
        )
    elif sql.dialect == "sqlite":
        sql.run_sql(
            f"create table {schema}.{table_name} (id decimal primary key, data varchar(10))"
        )
    else:
        metadata = MetaData(sql.get_engine())
        Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(10)),
            schema=schema,
        )
        metadata.create_all()


def test_get_pk(sql: DataCheckSql):
    create_test_table("test_upsert_get_pk", "main", sql)
    pk = sql.table_info.get_primary_keys("test_upsert_get_pk", "main")
    assert pk == ["id"]


def test_get_table(sql: DataCheckSql):
    create_test_table("test_upsert_get_table", "main", sql)
    table = sql.table_info.get_table("test_upsert_get_table", "main")
    assert len(list(table.columns)) == 2


def test_upsert(sql: DataCheckSql):
    df = pd.DataFrame.from_dict(
        {
            "id": [1, 2, 3],
            "data": ["a", "b", "c"],
        }
    )
    df2 = pd.DataFrame.from_dict(
        {
            "id": [2, 3, 4],
            "data": ["b2", "c2", "d"],
        }
    )
    df_expected = pd.DataFrame.from_dict(
        {
            "id": [1, 2, 3, 4],
            "data": ["a", "b2", "c2", "d"],
        }
    )
    create_test_table("test_upsert_1", "main", sql)
    sql.table_loader.load_table("main.test_upsert_1", df, LoadMode.APPEND)
    sql.table_loader.load_table("main.test_upsert_1", df2, LoadMode.UPSERT)

    data = sql.run_query("select * from main.test_upsert_1")

    assert_frame_equal(data, df_expected)
