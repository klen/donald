import asyncio

from donald import Donald, logger

manager = Donald(
    log_level="DEBUG",
    worker={"max_tasks": 2},
)


@manager.task
async def async_task(res=42, timeout=None, *, error=False):
    if timeout:
        await asyncio.sleep(timeout)

    if error:
        raise Exception("Task failed")

    logger.info("Run async_task %s", res)
    return res


@manager.schedule(1e-1)
@manager.task
async def scheduled_task(res=42):
    logger.info("Run scheduled_task %s", res)


@manager.task
async def nested_task():
    async_task.submit("nested")
    async_task.submit("nested")
    async_task.submit("nested")
    return "done"


@manager.task(bind=True)
async def bind_task(self):
    logger.info("Run bind_task %s", self.__class__.__name__)
    return self
