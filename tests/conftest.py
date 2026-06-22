import asyncio
import os

import pytest


@pytest.fixture()
def checklog(caplog):
    def _log_contains(msg, min_count=1):
        msgs = [r.message for r in caplog.records]
        return msgs.count(msg) >= min_count

    return _log_contains


@pytest.fixture()
def sleep():
    async def _sleep(timeout=1e-2):
        await asyncio.sleep(timeout)

    return _sleep


@pytest.fixture(scope="session")
def redis_url():
    """Provide a Redis URL for integration tests.

    Defaults to localhost. Set REDIS_URL to override.
    """
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return url
