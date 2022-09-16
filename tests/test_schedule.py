import asyncio
from typing import Counter

from .tasks import manager


def test_base():
    assert manager.scheduler._schedule


async def test_schedule(caplog):
    async with manager:
        w = manager.create_worker()
        w.start()

        manager.scheduler.start()
        assert manager.scheduler._tasks

        await asyncio.sleep(5e-1)

        await manager.scheduler.stop()
        assert not manager.scheduler._tasks

        await w.stop()

    log_messages = Counter(r.message for r in caplog.records)
    assert log_messages["Run scheduled_task 42"] == 4
