from donald import logger

from .tasks import async_task, fail, manager


async def test_on_start(sleep, checklog):
    @manager.on_start
    async def on_start():
        logger.info("Run on_start")

    async with manager:
        w = manager.create_worker()
        w.start()

        await async_task.submit()
        await sleep(1e-1)
        assert checklog("Run async_task 42")

        await w.stop()

    assert checklog("Run on_start")


async def test_on_stop(checklog, sleep):
    @manager.on_stop
    async def on_stop():
        logger.info("Run on_stop")

    async with manager:
        w = manager.create_worker()
        w.start()

        await async_task.submit()

        await w.join()
        assert checklog("Run async_task 42")
        await w.stop()

    assert checklog("Run on_stop")


async def test_on_error(checklog, sleep):
    @manager.on_error
    async def on_error(exc):
        logger.info("Run on_error: %s", exc)

    async with manager:
        w = manager.create_worker()
        w.start()

        await async_task.submit(error=True)

        await w.join()
        await w.stop()

    assert checklog("Run on_error: Task failed")


async def test_retry(checklog, sleep, caplog):

    async with manager:
        w = manager.create_worker()
        w.start()

        await fail.submit()
        await w.join()
        await w.stop()

    assert checklog("Run fail", fail._params["retries_max"] + 1)
    assert checklog("Run failback")
