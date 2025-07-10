from __future__ import annotations

import asyncio
from asyncio import sleep
from asyncio.locks import Event
from asyncio.tasks import create_task, gather
from datetime import timedelta
from typing import TYPE_CHECKING, Callable, Coroutine, Optional

from crontab import CronTab

from donald.tasks import TaskWrapper

from .utils import SchedulerNotReadyError, logger

if TYPE_CHECKING:
    from donald.types import TInterval


class Scheduler:
    """Manage periodic and cron-based task schedules."""

    def __init__(self) -> None:
        self._schedule: list[Callable[[], Coroutine]] = []
        self._tasks: list[asyncio.Task] = []
        self._finished: Optional[Event] = None

    def start(self):
        """Start all scheduled tasks."""
        logger.info("Starting task scheduler")
        self._finished = Event()
        for scheduler in self._schedule:
            self._tasks.append(create_task(scheduler()))

    async def stop(self):
        """Stop all scheduled tasks."""
        for task in self._tasks:
            task.cancel()
        await self.join()
        if self._finished is not None:
            self._finished.set()

    def wait(self):
        """Wait for scheduler to finish."""
        if self._finished is None:
            raise SchedulerNotReadyError
        return self._finished.wait()

    async def join(self, timeout: float | None = None):
        """Wait for all scheduler tasks to complete with optional timeout."""
        try:
            await asyncio.wait_for(
                gather(*self._tasks, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Scheduler join timed out")
        self._tasks.clear()

    def schedule(  # noqa: C901
            self, interval: TInterval, *, run_immediately: bool = False, backoff: float = 0
        ) -> Callable[[TaskWrapper], TaskWrapper]:
        """
        Schedule a task to run periodically with optional immediate run and backoff on failure.
        """

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

            async def submit():
                """Submit the task to the event loop."""
                try:
                    task.submit()
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "Immediate run of '%s' failed",
                        task.import_path(task._fn), exc_info=exc)
                    if backoff:
                        await sleep(backoff)

            async def scheduler():
                if run_immediately:
                    await submit()

                while True:
                    to_sleep = max(timer(), 1e-2)
                    logger.info("Next '%s' in %0.2f s", task.import_path(task._fn), to_sleep)
                    await sleep(to_sleep)
                    await submit()

            self._schedule.append(scheduler)
            return task

        return wrapper
