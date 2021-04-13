import pytest
import os

@pytest.fixture
def clickhouse_dsn():
    clickhouse_dsn = os.environ.get("BAMBOO_CLICKHOUSE_DSN_TEST")
    if not clickhouse_dsn:
        raise pytest.skip()
    return clickhouse_dsn
