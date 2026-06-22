import asyncio

import pytest

from .tasks import TaskSet, make_manager


@pytest.fixture(autouse=True)
async def donald():
    manager = make_manager()
    tasks = TaskSet(manager)
    async with manager:
        w1 = manager.create_worker()
        w2 = manager.create_worker()
        w1.start()
        w2.start()
        yield {"manager": manager, "tasks": tasks}
        await w1.stop()
        await w2.stop()


async def test_submit_task(sleep, checklog, donald):
    tasks = donald["tasks"]
    res = tasks.async_task.submit()
    assert await res

    await sleep(1e-1)

    assert checklog("Run async_task 42")

    for n in range(3):
        assert await tasks.async_task.submit(n)

    await sleep(1e-1)
    assert checklog("Run async_task 0")
    assert checklog("Run async_task 1")
    assert checklog("Run async_task 2")


async def test_delay(sleep, checklog, donald):
    tasks = donald["tasks"]
    res = tasks.async_task.apply_submit(delay=2e-1)
    assert await res

    await sleep(1e-1)
    assert not checklog("Run async_task 42")
    await sleep(2e-1)

    assert checklog("Run async_task 42")


async def test_timeout(sleep, checklog, donald):
    tasks = donald["tasks"]
    await tasks.async_task.apply_submit(timeout=1e-1, kwargs={"timeout": 2e-1})
    await sleep(3e-1)
    assert not checklog("Run async_task 42")


async def test_nested_tasks(sleep, checklog, donald):
    tasks = donald["tasks"]
    tasks.nested_task.submit()
    await sleep(3e-2)
    assert checklog("Run async_task nested", min_count=3)


async def test_binded_task(sleep, checklog, donald):
    tasks = donald["tasks"]
    tasks.bind_task.submit()
    await sleep()
    assert checklog("Run bind_task TaskRun")


async def test_submit_and_wait(donald, sleep, checklog):
    tasks = donald["tasks"]
    res = await tasks.async_task.submit_and_wait()
    assert res == 42

    assert checklog("Run async_task 42")


async def test_health_check(donald, sleep, checklog):
    manager = donald["manager"]
    res = await manager.healthcheck()
    assert res is True


async def test_concurrent_submit_and_wait(donald):
    tasks = donald["tasks"]
    results = await asyncio.gather(
        tasks.async_task.submit_and_wait(1),
        tasks.async_task.submit_and_wait(2),
        tasks.async_task.submit_and_wait(3),
    )
    assert results == [1, 2, 3]


async def test_subscribe_preserves_pending_messages(sleep):
    # Submit a task before subscribing; the queue must not be recreated on subscribe().
    manager = make_manager()
    tasks = TaskSet(manager)
    async with manager:
        submit_task = tasks.async_task.submit()
        await submit_task  # ensure the message is in the queue
        assert manager._backend.rx.qsize() == 1

        # subscribe() calls connect() internally; it must preserve existing messages.
        iterator = await manager._backend.subscribe()
        assert manager._backend.rx.qsize() == 1

        async for msg in iterator:
            path, args, _kwargs, _params = msg
            assert path == "tests.tasks.async_task"
            assert args == ()
            break
