import asyncio

from donald import logger

from .tasks import async_task, manager


async def test_on_start(check, caplog):
    async def on_start():
        logger.info("Run on_start")

    manager.setup(worker_params={"on_start": on_start})

    async with manager:
        w = manager.create_worker()
        w.start()

        await async_task.submit(check.path)
        await asyncio.sleep(1e-1)
        assert check() == ["42"]

        await w.stop()

    log_messages = {r.message for r in caplog.records}
    assert "Run on_start" in log_messages


async def test_on_stop(check, caplog):
    async def on_stop():
        logger.info("Run on_stop")

    manager.setup(worker_params={"on_stop": on_stop})

    async with manager:
        w = manager.create_worker()
        w.start()

        await async_task.submit(check.path)
        await asyncio.sleep(1e-1)
        assert check() == ["42"]

        await w.stop()

    log_messages = {r.message for r in caplog.records}
    assert "Run on_stop" in log_messages


async def test_on_error(check, caplog):
    async def on_error(exc):
        logger.info("Run on_error: %s", exc)

    manager.setup(worker_params={"on_error": on_error})

    async with manager:
        w = manager.create_worker()
        w.start()

        await async_task.submit(check.path, error=True)
        await asyncio.sleep(1e-1)

        await w.stop()

    log_messages = {r.message for r in caplog.records}
    assert "Run on_error: Task failed" in log_messages