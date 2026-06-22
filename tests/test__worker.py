import time

import pytest

from donald import logger
from donald.utils import current_manager

from .tasks import TaskSet, make_manager


@pytest.fixture
async def worker_fixtures():
    manager = make_manager()
    tasks = TaskSet(manager)
    return {"manager": manager, "tasks": tasks}


async def test_on_start(worker_fixtures, sleep, checklog):
    manager = worker_fixtures["manager"]
    tasks = worker_fixtures["tasks"]

    @manager.on_start
    async def on_start():
        logger.info("Run on_start")

    async with manager:
        w = manager.create_worker()
        w.start()

        await tasks.async_task.submit()
        await sleep(1e-1)
        assert checklog("Run async_task 42")

        await w.stop()

    assert checklog("Run on_start")


async def test_on_stop(worker_fixtures, checklog, sleep):
    manager = worker_fixtures["manager"]
    tasks = worker_fixtures["tasks"]

    @manager.on_stop
    async def on_stop():
        logger.info("Run on_stop")

    async with manager:
        w = manager.create_worker()
        w.start()

        await tasks.async_task.submit()

        await w.join()
        assert checklog("Run async_task 42")
        await w.stop()

    assert checklog("Run on_stop")


async def test_on_error(worker_fixtures, checklog, sleep):
    manager = worker_fixtures["manager"]
    tasks = worker_fixtures["tasks"]

    @manager.on_error
    async def on_error(exc):
        logger.info("Run on_error: %s", exc)

    async with manager:
        w = manager.create_worker()
        w.start()

        await tasks.async_task.submit(error=True)

        await w.join()
        await w.stop()

    assert checklog("Run on_error: Task failed")


async def test_retry(worker_fixtures, checklog, sleep, caplog):
    manager = worker_fixtures["manager"]
    tasks = worker_fixtures["tasks"]

    async with manager:
        w = manager.create_worker()
        w.start()

        await tasks.fail.submit()
        await w.join()
        await w.stop()

    assert checklog("Run fail", tasks.fail._params["retries_max"] + 1)
    assert checklog("Run failback")


async def test_bind_retry(worker_fixtures, checklog, sleep):
    manager = worker_fixtures["manager"]
    tasks = worker_fixtures["tasks"]

    # Simulate a worker context where the global current_manager is not set
    # (e.g., a fresh process or a different manager instance).
    previous = current_manager.value
    current_manager.value = None
    try:
        async with manager:
            w = manager.create_worker()
            w.start()

            await tasks.retry_once.submit()
            await w.join()
            await w.stop()
    finally:
        current_manager.value = previous

    assert checklog("Run retry_once retries=0")
    assert checklog("Run retry_once retries=1")


async def test_retry_delay_not_accumulated():
    # The initial delay should apply only once, not on every retry attempt.
    manager = make_manager()
    tasks = TaskSet(manager)
    start = time.monotonic()
    async with manager:
        w = manager.create_worker()
        w.start()

        await tasks.retry_with_delay.submit()
        await w.join()
        await w.stop()

    elapsed = time.monotonic() - start
    # With accumulation the task would sleep 3 * 0.2 = 0.6s; without it ~0.2s.
    assert elapsed < 0.5
