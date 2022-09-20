import asyncio

import pytest
from redis import Redis


@pytest.fixture
def checklog(caplog):
    def _log_contains(msg, min_count=1):
        msgs = [r.message for r in caplog.records]
        return msgs.count(msg) >= min_count

    return _log_contains


@pytest.fixture
def sleep():
    async def _sleep(timeout=1e-2):
        await asyncio.sleep(timeout)

    return _sleep


@pytest.fixture
def redis(redis_url):
    return Redis.from_url(redis_url)
