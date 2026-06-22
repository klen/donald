import asyncio

from donald import Donald, logger
from donald.utils import current_manager

# Module-level TaskWrappers are kept for worker import compatibility.
# Tests should use make_manager() + TaskSet to avoid shared state.
_default_manager = Donald(log_level="DEBUG", worker={"max_tasks": 2})


@_default_manager.task()
async def async_task(res=42, timeout=None, *, error=False):
    if timeout:
        await asyncio.sleep(timeout)

    if error:
        raise Exception("Task failed")

    logger.info("Run async_task %s", res)
    return res


@_default_manager.task()
async def scheduled_task(res=42):
    logger.info("Run scheduled_task %s", res)


scheduled_task.schedule(1e-1)


@_default_manager.task()
async def nested_task():
    manager = current_manager.value
    manager.submit(async_task._fn, "nested")
    manager.submit(async_task._fn, "nested")
    manager.submit(async_task._fn, "nested")
    return "done"


@_default_manager.task(bind=True)
async def bind_task(self):
    logger.info("Run bind_task %s", self.__class__.__name__)
    return self


@_default_manager.task(retries_max=3, retries_backoff_factor=1e-1)
async def fail():
    logger.error("Run fail")
    raise Exception("Task failed")


@_default_manager.task(retries_max=2, delay=2e-1, retries_backoff_factor=0)
async def retry_with_delay():
    logger.info("Run retry_with_delay")
    raise Exception("Task failed")


@_default_manager.task(bind=True)
async def retry_once(self):
    logger.info("Run retry_once retries=%s", self.retries)
    if self.retries == 0:
        await self.retry()
    return self.retries


@fail.failback()
async def failback(exc, *args, **kwargs):
    logger.error("Run failback")
    return exc


class TaskSet:
    """A fresh set of tasks bound to a given manager."""

    def __init__(self, manager: Donald):
        self.manager = manager
        self.async_task = manager.task()(async_task._fn)
        self.scheduled_task = manager.task()(scheduled_task._fn)
        self.scheduled_task.schedule(1e-1)
        self.nested_task = manager.task()(nested_task._fn)
        self.bind_task = manager.task(bind=True)(bind_task._fn)
        self.fail = manager.task(retries_max=3, retries_backoff_factor=1e-1)(fail._fn)
        self.fail.failback()(failback)
        self.retry_with_delay = manager.task(retries_max=2, delay=2e-1, retries_backoff_factor=0)(
            retry_with_delay._fn
        )
        self.retry_once = manager.task(bind=True)(retry_once._fn)


def make_manager() -> Donald:
    """Create a fresh Donald manager for tests."""
    return Donald(log_level="DEBUG", worker={"max_tasks": 2})
