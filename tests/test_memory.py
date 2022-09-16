import asyncio

import pytest

from donald.core import TaskResult

from .tasks import async_task, manager


@pytest.fixture()
async def setup():
    async with manager:
        w1 = manager.create_worker()
        w2 = manager.create_worker()
        w1.start()
        w2.start()
        yield manager
        await w1.stop()
        await w2.stop()


async def test_submit_task(setup, check):
    res = async_task.submit(check.path)
    assert isinstance(res, TaskResult)
    await res

    submited = await res
    assert submited

    await asyncio.sleep(1e-1)

    assert check() == ["42"]

    for n in range(10):
        assert await async_task.submit(check.path, n)

    await asyncio.sleep(1e-1)
    assert check() == ["42", *map(str, range(10))]


async def test_delay(setup, check):
    res = async_task.apply_submit(check.path, delay=2e-1)
    assert isinstance(res, TaskResult)

    await asyncio.sleep(1e-1)
    assert check() is None
    await asyncio.sleep(2e-1)

    assert check() == ["42"]


async def test_timeout(setup, check):
    await async_task.apply_submit(check.path, timeout=1e-1, kwargs={"timeout": 2e-1})
    await asyncio.sleep(3e-1)
    assert check() is None
