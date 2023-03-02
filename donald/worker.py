from __future__ import annotations

import os
from asyncio import iscoroutine
from asyncio.exceptions import CancelledError
from asyncio.locks import Event, Semaphore
from asyncio.tasks import Task, create_task, gather, sleep
from numbers import Number
from typing import AsyncIterator, Callable, Dict, Iterable, Optional, Set, cast

from async_timeout import timeout as async_timeout

from . import __version__, logger
from .backend import BaseBackend
from .types import TRunArgs, TTaskParams, TWorkerParams
from .utils import import_obj

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
    defaults: TWorkerParams = {
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
        self._params = cast(TWorkerParams, dict(self.defaults, **params))
        self._task_params = cast(TTaskParams, self._params["task_defaults"] or {})

        self.on_error = self._params.get("on_error")

        max_tasks = self._params["max_tasks"]
        self._sem = max_tasks and Semaphore(max_tasks - 1)

        self._tasks: Set[Task] = set()
        self._finished = Event()

    def start(self):
        """Start the worker."""
        msg = self._params.get("show_banner") and BANNER or ""
        msg += f"\n\nDonald v{__version__} - Worker"
        msg += f"\nBackend: {self._backend.type}"
        msg += f"\nPID: {os.getpid()}\n"

        logger.info(msg)
        self._finished.clear()
        self._runner = create_task(self.run_worker(), name="Worker Run Task")
        self._runner.add_done_callback(self.finish_task)

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
            try:
                path, args, kwargs, params = task_msg
                tw = import_obj(path)
            except Exception as exc:
                logger.exception("Failed to get task: %s", path, exc_info=exc)
                continue

            task_params = dict(self._task_params, **params)
            name = tw.import_path()
            task = create_task(
                self.run_task(
                    tw,
                    args,
                    kwargs,
                    timeout=task_params.pop("timeout", None),  # type: ignore
                    delay=task_params.pop("delay", None),  # type: ignore
                    **task_params,
                ),
                name=name,
            )
            tasks.add(task)
            task.add_done_callback(finish_task)
            logger.info("Run: '%s' (%d)", name, id(task))
            if sem:
                await sem.acquire()

    async def run_task(
        self,
        corofunc: Callable,
        args: Iterable,
        kwargs: Dict,
        *,
        timeout: Optional[Number] = None,
        delay: Optional[Number] = None,
        **params,
    ):
        # Process delay
        if delay:
            await sleep(cast(float, delay))

        # Process timeout
        if timeout:
            async with async_timeout(cast(float, timeout)):
                return await corofunc(*args, **kwargs)

        return await corofunc(*args, **kwargs)

    def finish_task(self, task: Task):
        try:
            exc = task.exception()
            if exc:
                logger.exception(
                    "Fail: '%s' (%d)", task.get_name(), id(task), exc_info=exc
                )
                if self.on_error:
                    coro = self.on_error(exc)
                    if iscoroutine(coro):
                        create_task(coro)
        except CancelledError:
            pass

        if task in self._tasks:
            self._tasks.remove(task)
            if self._sem:
                self._sem.release()
        logger.info("Finished: '%s' (%d)", task.get_name(), id(task))

    async def join(self):
        """Wait for all tasks to complete."""
        tasks = self._tasks
        if tasks:
            logger.info("Waiting for %d tasks to complete", len(self._tasks))
            return await gather(*tasks, return_exceptions=True)
