from data_check.sql import DataCheckSql, Table


def test_parse_table_name_with_schema(sql: DataCheckSql):
    table = Table.from_table_name(sql, "a.b")
    assert table.schema == "a"
    assert table.name == "b"


def test_parse_table_name(sql: DataCheckSql):
    table = Table.from_table_name(sql, "b")
    assert table.schema is None
    assert table.name == "b"


def test_parse_table_name_dot(sql: DataCheckSql):
    table = Table.from_table_name(sql, ".")
    assert table.schema == ""
    assert table.name == ""


def test_parse_table_name_with_schema_with_db(sql: DataCheckSql):
    table = Table.from_table_name(sql, "db.a.b")
    # this looks wrong, but multiple databases are currently not supported
    assert table.schema == "db"
    assert table.name == "a.b"
