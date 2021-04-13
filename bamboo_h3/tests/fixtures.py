import os

import pytest
from bamboo_h3 import ClickhouseConnection


def __clickhouse_dsn():
    clickhouse_dsn = os.environ.get("BAMBOO_CLICKHOUSE_DSN_TEST")
    if not clickhouse_dsn:
        raise pytest.skip()
    return clickhouse_dsn


@pytest.fixture
def clickhouse_dsn():
    return __clickhouse_dsn()


@pytest.fixture
def clickhouse_db():
    return ClickhouseConnection(__clickhouse_dsn())
