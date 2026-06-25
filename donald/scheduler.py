from __future__ import annotations

import asyncio
import json
import os
from asyncio import sleep
from asyncio.locks import Event
from asyncio.tasks import create_task, gather
from contextlib import suppress
from datetime import timedelta
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Callable, Coroutine

from crontab import CronTab

from donald.tasks import TaskWrapper

from .utils import ManagerNotReadyError, SchedulerNotReadyError, logger

if TYPE_CHECKING:
    from donald.types import TInterval

HEARTBEAT_DEFAULT_PATH = "/tmp/donald-scheduler.heartbeat"  # noqa: S108


class Scheduler:
    """Manage periodic and cron-based task schedules."""

    def __init__(
        self,
        heartbeat_interval: float = 60.0,
        heartbeat_file: str = HEARTBEAT_DEFAULT_PATH,
    ) -> None:
        self._ready = Event()
        self._stop = Event()

        self._schedule: list[Callable[[], Coroutine]] = []
        self._tasks: list[asyncio.Task] = []

        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_file = heartbeat_file

    def start(self):
        """Start all scheduled tasks and heartbeat."""
        logger.info("Starting task scheduler")

        for scheduler_coro in self._schedule:
            self._tasks.append(create_task(scheduler_coro()))

        if self.heartbeat_interval > 0:
            heartbeat = create_task(self._run_heartbeat(), name="scheduler-heartbeat")
            heartbeat.add_done_callback(lambda _: self._clear_heartbeat())
            self._tasks.append(heartbeat)

        self._ready.set()
        self._stop.clear()

    async def stop(self):
        """Stop all scheduled tasks and heartbeat."""
        self._stop.set()
        self._ready.clear()

        for task in self._tasks:
            task.cancel()

        await self.join()

    async def _run_heartbeat(self) -> None:
        """Write heartbeat to file periodically."""
        while self._ready.is_set() and not self._stop.is_set():
            with suppress(OSError):
                Path(self.heartbeat_file).write_text(
                    json.dumps({"pid": os.getpid(), "timestamp": time()})
                )
            try:
                await sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                return

    def wait(self):
        """Wait for scheduler to finish."""
        if not self._ready.is_set():
            raise SchedulerNotReadyError
        return self._stop.wait()

    async def join(self, timeout: float | None = None):
        """Wait for all scheduler tasks to complete with optional timeout."""
        try:
            fut = gather(*self._tasks, return_exceptions=True)
            await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Scheduler join timed out")
        self._tasks.clear()

    def submit(self, task: TaskWrapper):
        """Submit a task to the event loop."""
        if not self._ready.is_set():
            raise SchedulerNotReadyError
        try:
            task.submit()
        except ManagerNotReadyError as exc:
            self._stop.set()
            raise SchedulerNotReadyError("Manager not ready, stopping scheduler") from exc
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to submit task '%s'", task.import_path(task._fn), exc_info=exc)

    def _clear_heartbeat(self) -> None:
        """Remove the heartbeat file on shutdown."""
        with suppress(OSError):
            Path(self.heartbeat_file).unlink()

    def schedule(
        self, interval: TInterval, *, run_immediately: bool = False
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
            fn_path = task.import_path(task._fn)

            async def submit():
                """Submit the task to the event loop."""
                logger.info("Submitting task '%s'", fn_path)
                self.submit(task)

            async def scheduler_coro():
                if run_immediately:
                    self.submit(task)

                while True:
                    to_sleep = max(timer(), 1e-2)
                    logger.info("Next run of '%s': %s", fn_path, timedelta(seconds=to_sleep))
                    await sleep(to_sleep)
                    self.submit(task)

            self._schedule.append(scheduler_coro)
            return task

        return wrapper
