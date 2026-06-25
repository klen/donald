import asyncio

from .tasks import TaskSet, make_manager


def test_base():
    manager = make_manager()
    tasks = TaskSet(manager)
    assert tasks.manager.scheduler._schedule


async def test_schedule(checklog, sleep):
    manager = make_manager()
    tasks = TaskSet(manager)
    manager.scheduler.heartbeat_interval = 0  # disable heartbeat for this test
    async with manager:
        w = manager.create_worker()
        w.start()

        manager.scheduler.start()
        assert manager.scheduler._tasks

        await sleep(5e-1)

        await manager.scheduler.stop()
        assert not manager.scheduler._tasks

        await w.stop()

    assert checklog("Run scheduled_task 42", 4)


async def test_scheduler_healthcheck_not_started():
    """Before scheduler starts, heartbeat file doesn't exist → unhealthy."""
    manager = make_manager()
    TaskSet(manager)
    assert await manager.scheduler_healthcheck() is False


async def test_scheduler_healthcheck_started_and_stopped():
    """After start → healthy, after stop → heartbeat file removed → unhealthy."""
    manager = make_manager()
    TaskSet(manager)
    await manager.start()
    manager.scheduler.heartbeat_interval = 0.1
    manager.scheduler.start()
    await asyncio.sleep(0.3)
    assert await manager.scheduler_healthcheck() is True
    await manager.stop()
    assert await manager.scheduler_healthcheck() is False


async def test_manager_scheduler_healthcheck(checklog, sleep):
    """Manager.scheduler_healthcheck() via heartbeat file."""
    manager = make_manager()
    tasks = TaskSet(manager)
    manager.scheduler.heartbeat_interval = 0.1
    await manager.start()

    w = manager.create_worker()
    w.start()

    manager.scheduler.start()

    await sleep(5e-1)

    assert await manager.scheduler_healthcheck() is True

    await w.stop()
    await manager.stop()

    assert await manager.scheduler_healthcheck() is False
