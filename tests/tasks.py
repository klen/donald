import asyncio
from functools import wraps

from donald import Donald, logger

manager = Donald(
    log_level="DEBUG",
    worker={"max_tasks": 2},
)


def store_result(fn):
    @wraps(fn)
    async def wrapper(tmp_path, *args, **kwargs):
        result = await fn(*args, **kwargs)
        with open(tmp_path / "result.txt", "a") as f:
            f.write(f"{result}\n")

    return wrapper


@manager.task
@store_result
async def async_task(res=42, timeout=None, error=False):
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
