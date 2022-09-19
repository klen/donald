import asyncio

import pytest

from donald.manager import TaskResult

from .tasks import async_task, manager


@pytest.fixture()
async def setup(redis_url):
    manager.setup(backend="redis", backend_params={"url": redis_url})
    assert manager._backend
    assert manager._backend.params

    async with manager:
        w1 = manager.create_worker()
        # w2 = manager.create_worker()
        w1.start()
        # w2.start()
        yield manager
        await w1.stop()
        # await w2.stop()


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
