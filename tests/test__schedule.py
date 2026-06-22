from .tasks import TaskSet, make_manager


def test_base():
    manager = make_manager()
    tasks = TaskSet(manager)
    assert tasks.manager.scheduler._schedule


async def test_schedule(checklog, sleep):
    manager = make_manager()
    tasks = TaskSet(manager)
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
