from __future__ import annotations

from asyncio import sleep
from asyncio.locks import Event
from asyncio.tasks import create_task, gather
from datetime import timedelta
from typing import Callable, Union

from crontab import CronTab

from donald.tasks import TaskWrapper

from .utils import SchedulerNotReadyError, logger

TInterval = Union[timedelta, int, float, str, CronTab]


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
            raise SchedulerNotReadyError
        return self._finished.wait()

    async def join(self):
        await gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def schedule(self, interval: TInterval) -> Callable[[TaskWrapper], TaskWrapper]:
        """Schedule a task to run periodically."""

        assert not callable(interval), "Use @manager.schedule(interval)"

        if isinstance(interval, str):
            interval = CronTab(interval)

        if isinstance(interval, timedelta):
            timer = interval.total_seconds

        elif isinstance(interval, CronTab):
            timer = lambda: interval.next(default_utc=True)  # type: ignore[union-attr]

        else:
            timer = lambda: float(interval)  # type: ignore[arg-type]

        def wrapper(task: TaskWrapper):
            assert isinstance(task, TaskWrapper), "Only tasks can be scheduled."

            async def scheduler():
                while True:
                    to_sleep = max(timer(), 1e-2)
                    logger.info("Next '%s' in %0.2f s", task.import_path(), to_sleep)
                    await sleep(to_sleep)
                    task.submit()

            self._schedule.append(scheduler)
            return task

        return wrapper
