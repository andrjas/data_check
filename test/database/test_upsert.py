import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy import Table as SQLTable

from data_check.sql import DataCheckSql, Table
from data_check.sql.load_mode import LoadMode


def create_test_table(table_name: str, schema: str, sql: DataCheckSql):
    table = Table(sql, table_name, schema)
    table.drop_if_exists()
    if sql.dialect == "oracle":
        sql.run_sql(
            f"create table {table} (id number(10) primary key, data varchar2(10))"
        )
    elif sql.dialect == "sqlite":
        sql.run_sql(f"create table {table} (id decimal primary key, data varchar(10))")
    else:
        metadata = MetaData()
        SQLTable(
            table.name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(10)),
            schema=table.schema,
        )
        metadata.create_all(bind=sql.get_engine())
    return table


def test_get_pk(sql: DataCheckSql):
    table = create_test_table("test_upsert_get_pk", "main", sql)
    assert table.primary_keys == ["id"]


def test_get_table(sql: DataCheckSql):
    table = create_test_table("test_upsert_get_table", "main", sql)
    sql_table = table.sql_table
    assert len(list(sql_table.columns)) == 2


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
    table = create_test_table("test_upsert_1", "main", sql)
    sql.table_loader.load_table(table, df, LoadMode.APPEND)
    sql.table_loader.load_table(table, df2, LoadMode.UPSERT)

    data = sql.run_query("select * from main.test_upsert_1")

    assert_frame_equal(data, df_expected)
