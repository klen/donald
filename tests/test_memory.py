import pytest

from .tasks import async_task, bind_task, manager, nested_task


@pytest.fixture(autouse=True)
async def setup():
    async with manager:
        w1 = manager.create_worker()
        w2 = manager.create_worker()
        w1.start()
        w2.start()
        yield manager
        await w1.stop()
        await w2.stop()


async def test_submit_task(sleep, checklog):
    res = async_task.submit()
    assert await res

    await sleep(1e-1)

    assert checklog("Run async_task 42")

    for n in range(3):
        assert await async_task.submit(n)

    await sleep(1e-1)
    assert checklog("Run async_task 0")
    assert checklog("Run async_task 1")
    assert checklog("Run async_task 2")


async def test_delay(sleep, checklog):
    res = async_task.apply_submit(delay=2e-1)
    assert await res

    await sleep(1e-1)
    assert not checklog("Run async_task 42")
    await sleep(2e-1)

    assert checklog("Run async_task 42")


async def test_timeout(sleep, checklog):
    await async_task.apply_submit(timeout=1e-1, kwargs={"timeout": 2e-1})
    await sleep(3e-1)
    assert not checklog("Run async_task 42")


async def test_nested_tasks(sleep, checklog):
    nested_task.submit()
    await sleep(2e-2)
    assert checklog("Run async_task nested", min_count=3)


async def test_binded_task(sleep, checklog):
    bind_task.submit()
    await sleep()
    assert checklog("Run bind_task TaskRun")
