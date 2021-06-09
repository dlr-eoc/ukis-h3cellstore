# noinspection PyUnresolvedReferences
from ..fixtures import clickhouse_db


def test_connection_fetch_dataframe(clickhouse_db):
    df = clickhouse_db.query_fetch("select 25 as col1").to_dataframe()
    assert df["col1"][0] == 25


def test_list_tablesets(clickhouse_db):
    # test should not raise
    tablesets = clickhouse_db.list_tablesets()
    assert type(tablesets) == dict
