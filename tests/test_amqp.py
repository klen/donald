import pytest

from .tasks import async_task, manager


@pytest.fixture
async def donald():
    manager.setup(backend="amqp")
    assert manager._backend
    assert manager._backend.params

    async with manager:
        w1 = manager.create_worker()
        w2 = manager.create_worker()
        w1.start()
        w2.start()
        yield manager
        await w1.stop()
        await w2.stop()


async def test_submit_task(donald, sleep, checklog):
    res = async_task.submit()
    assert res
    assert await res

    await sleep()
    assert checklog("Run async_task 42")

    for n in range(3):
        assert await async_task.submit(n)

    await sleep(1e-1)
    assert checklog("Run async_task 0")
    assert checklog("Run async_task 1")
    assert checklog("Run async_task 2")


async def test_submit_and_wait(donald, sleep, checklog):
    res = await async_task.submit_and_wait()
    assert res == 42

    assert checklog("Run async_task 42")


async def test_health_check(donald, sleep, checklog):
    res = await donald.healthcheck()
    assert res is True
