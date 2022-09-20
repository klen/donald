from .tasks import manager


def test_base():
    assert manager.scheduler._schedule


async def test_schedule(checklog, sleep):
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
