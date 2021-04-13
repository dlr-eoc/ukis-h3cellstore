# noinspection PyUnresolvedReferences
from .fixtures import clickhouse_dsn

from bamboo_h3 import ClickhouseConnection

def test_connection_fetch_dataframe(clickhouse_dsn):
    conn = ClickhouseConnection(clickhouse_dsn)
    assert conn is not None
    df = conn.query_fetch("select 25 as col1").to_dataframe()
    assert df["col1"][0] == 25
