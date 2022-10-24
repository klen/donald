from asyncio import sleep
from asyncio.locks import Event
from asyncio.tasks import create_task, gather
from datetime import timedelta
from numbers import Number
from typing import Callable, Union

from crontab import CronTab

from donald.tasks import TaskWrapper

from . import logger

TInterval = Union[timedelta, Number, CronTab]


class Scheduler:
    """Manage schedules."""

    def __init__(self):
        self._schedule = []
        self._tasks = []
        self._finished = None

    def start(self):
        logger.info("Starting task scheduler")
        self._finished = Event()
        for scheduler in self._schedule:
            self._tasks.append(create_task(scheduler()))

    async def stop(self):
        for task in self._tasks:
            task.cancel()

        await self.join()
        if self._finished is not None:
            self._finished.set()

    def wait(self):
        if self._finished is None:
            raise RuntimeError("Scheduler is not started.")
        return self._finished.wait()

    async def join(self):
        await gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def schedule(self, interval: TInterval) -> Callable[[TaskWrapper], TaskWrapper]:
        """Schedule a task to run periodically."""

        if callable(interval):
            raise RuntimeError("@manager.schedule(interval) should be used.")

        if isinstance(interval, str) and " " in interval:
            interval = CronTab(interval)

        if isinstance(interval, timedelta):
            timer = interval.total_seconds

        elif isinstance(interval, CronTab):
            timer = lambda: interval.next(default_utc=True)  # type: ignore

        else:
            timer = lambda: float(interval)  # type: ignore

        def wrapper(task: TaskWrapper):
            if not isinstance(task, TaskWrapper):
                raise RuntimeError("Only tasks can be scheduled.")

            async def scheduler():
                while True:
                    to_sleep = max(timer(), 1e-2)
                    logger.info("Next '%s' in %0.2f s", task.import_path(), to_sleep)
                    await sleep(to_sleep)
                    task.submit()

            self._schedule.append(scheduler)
            return task

        return wrapper
