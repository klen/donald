from __future__ import annotations

import os
from asyncio import iscoroutine
from asyncio.exceptions import CancelledError
from asyncio.locks import Event, Semaphore
from asyncio.tasks import Task, create_task, gather, sleep
from contextlib import suppress
from importlib import metadata
from random import random
from typing import TYPE_CHECKING, AsyncIterator, ClassVar, cast

from donald.tasks import TaskRun, TaskWrapper

from ._compat import async_timeout
from .utils import import_obj, logger

if TYPE_CHECKING:

    from .backend import BaseBackend
    from .types import TRunArgs, TTaskParams, TWorkerParams

BANNER = r"""

$$$$$$$\                                $$\       $$\
$$  __$$\                               $$ |      $$ |
$$ |  $$ | $$$$$$\  $$$$$$$\   $$$$$$\  $$ | $$$$$$$ |
$$ |  $$ |$$  __$$\ $$  __$$\  \____$$\ $$ |$$  __$$ |
$$ |  $$ |$$ /  $$ |$$ |  $$ | $$$$$$$ |$$ |$$ /  $$ |
$$ |  $$ |$$ |  $$ |$$ |  $$ |$$  __$$ |$$ |$$ |  $$ |
$$$$$$$  |\$$$$$$  |$$ |  $$ |\$$$$$$$ |$$ |\$$$$$$$ |
\_______/  \______/ \__|  \__| \_______|\__| \_______|
"""


class Worker:
    defaults: ClassVar[TWorkerParams] = {
        "max_tasks": 0,
        "task_defaults": None,
        "on_start": None,
        "on_stop": None,
        "on_error": None,
        "show_banner": False,
    }

    def __init__(self, backend: BaseBackend, params: TWorkerParams):
        self._backend = backend
        self._runner = None
        self._params = cast("TWorkerParams", dict(self.defaults, **params))
        self._task_params = cast("TTaskParams", self._params["task_defaults"] or {})

        self.on_error = self._params.get("on_error")

        max_tasks = self._params["max_tasks"]
        self._sem = (max_tasks and Semaphore(max_tasks)) or None

        self._tasks: set[Task] = set()
        self._finished = Event()

    def start(self):
        """Start the worker."""
        msg = (self._params.get("show_banner") and BANNER) or ""
        msg += f"\n\nDonald v{metadata.version('donald')} - Worker"
        msg += f"\nBackend: {self._backend.backend_type}"
        msg += f"\nPID: {os.getpid()}\n"

        logger.info(msg)
        self._finished.clear()
        self._runner = create_task(self.run_worker(), name="Worker Run Task")
        self._runner.add_done_callback(self.finish_runner)

    async def stop(self):
        """Stop the worker."""
        logger.info("Stopping worker")
        for task in self._tasks:
            task.cancel()

        if self._runner and not self._runner.done():
            self._runner.add_done_callback(lambda _: self._finished.set())
            self._runner.cancel()

        await self.join()

        on_stop = self._params.get("on_stop")
        if on_stop:
            await on_stop()

        self._finished.set()

    def wait(self):
        return self._finished.wait()

    async def run_worker(self: Worker):
        on_start = self._params.get("on_start")
        if on_start:
            await on_start()

        logger.info("Worker started")
        task_iter: AsyncIterator[TRunArgs] = await self._backend.subscribe()
        sem, tasks, finish_task = self._sem, self._tasks, self.finish_task
        async for task_msg in task_iter:
            path, args, kwargs, params = task_msg
            try:
                tw = import_obj(path)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to get task: %s", path, exc_info=exc)
                continue

            if not isinstance(tw, TaskWrapper):
                tw = TaskWrapper(None, tw)

            task_params = cast("TTaskParams", dict(self._task_params, **params))
            task = create_task(self.run_task(tw, args, kwargs, **task_params), name=path)
            tasks.add(task)
            task.add_done_callback(finish_task)
            logger.info("Run: '%s' (%d)", path, id(task))
            if sem:
                await sem.acquire()

    async def run_task(  # noqa: PLR0913
        self,
        tw: TaskWrapper,
        args: tuple,
        kwargs: dict,
        *,
        retries: int = 0,
        bind: bool = False,
        delay: float = 0,
        timeout: float = 0,
        retries_max: int = 0,
        retries_backoff_max: float = 600,
        retries_backoff_factor: float = .5,
        reply_to: str | None = None,
        correlation_id: str | None = None,
    ):
        """Run a task with the given parameters."""
        # Process delay
        if delay:
            await sleep(cast("float", delay))

        # Process timeout
        try:
            if timeout:
                async with async_timeout(cast("float", timeout)):
                    result = await tw._fn(*args, **kwargs)
            else:
                result = await tw._fn(*args, **kwargs)

            if reply_to and correlation_id:
                await self._backend.callback(result, reply_to, correlation_id)

            return result

        except Exception as exc:
            if bind:
                task_run: TaskRun = args[0]
                task_run.retries += 1
                retries = task_run.retries
            else:
                retries += 1

            if retries <= retries_max:
                backoff = min(retries_backoff_max, retries_backoff_factor * (2 ** (retries - 1) + random()))  # noqa: S311, E501
                logger.info("Retry: '%s' (%d) in %.2fs", tw._fn.__qualname__, retries, backoff)
                return await self.run_task(
                    tw,
                    args,
                    kwargs,
                    retries=retries,
                    bind=bind,
                    delay=delay + backoff,
                    timeout=timeout,
                    retries_max=retries_max,
                    retries_backoff_factor=retries_backoff_factor,
                    reply_to=reply_to,
                    correlation_id=correlation_id,
                )

            if tw._failback:
                return await tw._failback(exc, *args, **kwargs)

            raise

    def finish_task(self, task: Task):
        """Callback to handle the completion of a task."""
        self._tasks.discard(task)
        if task.cancelled():
            logger.info("Task cancelled: '%s'", task.get_name())

        with suppress(CancelledError):
            exc = task.exception()
            if exc:
                logger.exception("Fail: '%s' (%d)", task.get_name(), id(task), exc_info=exc)
                if self.on_error:
                    coro = self.on_error(exc)
                    if iscoroutine(coro):
                        create_task(coro)

        if self._sem:
            self._sem.release()

        logger.info("Finished: '%s' (%d)", task.get_name(), id(task))

    def finish_runner(self, runner: Task):
        """Callback to handle the completion of the worker runner."""
        with suppress(CancelledError):
            exc = runner.exception()
            if exc:
                logger.exception("Worker runner failed", exc_info=exc)
            else:
                logger.info("Worker runner finished successfully")
        self._finished.set()

    async def join(self):
        """Wait for all tasks to complete."""
        tasks = self._tasks
        if tasks:
            logger.info("Waiting for %d tasks to complete", len(self._tasks))
            return await gather(*tasks, return_exceptions=True)
        return None
