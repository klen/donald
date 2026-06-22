from uuid import uuid4

import pytest
import redis

from .tasks import TaskSet, make_manager


def _redis_available(url: str) -> bool:
    try:
        client = redis.Redis.from_url(url, socket_connect_timeout=1)
        return client.ping()
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture
async def donald(redis_url):
    if not _redis_available(redis_url):
        pytest.skip(f"Redis is not available at {redis_url}")

    manager = make_manager()
    tasks = TaskSet(manager)
    manager.setup(backend="redis", backend_params={"url": redis_url, "channel": uuid4()})
    assert manager._backend
    assert manager._backend.params

    async with manager:
        w1 = manager.create_worker()
        w1.start()
        yield {"manager": manager, "tasks": tasks}
        await w1.stop()


async def test_submit_task(donald, sleep, checklog):
    tasks = donald["tasks"]
    res = tasks.async_task.submit()
    assert res
    assert await res

    await sleep(1e-1)

    assert checklog("Run async_task 42")

    for n in range(3):
        assert await tasks.async_task.submit(n)

    await sleep(1e-1)
    assert checklog("Run async_task 0")
    assert checklog("Run async_task 1")
    assert checklog("Run async_task 2")


async def test_submit_and_wait(donald, sleep, checklog):
    tasks = donald["tasks"]
    res = await tasks.async_task.submit_and_wait()
    assert res == 42

    assert checklog("Run async_task 42")


async def test_health_check(donald, sleep, checklog):
    manager = donald["manager"]
    res = await manager.healthcheck()
    assert res is True
