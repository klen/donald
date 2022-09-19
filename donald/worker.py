from __future__ import annotations

import os
from asyncio.exceptions import CancelledError
from asyncio.locks import Event, Semaphore
from asyncio.tasks import Task, create_task, gather, sleep
from inspect import iscoroutinefunction
from numbers import Number
from typing import AsyncIterator, Callable, Dict, Iterable, Set, cast

from async_timeout import timeout as async_timeout

from . import __version__, logger
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
    defaults: TWorkerParams = {
        "show_banner": False,
        "max_tasks": 0,
        "task_defaults": None,
        "on_start": None,
        "on_stop": None,
        "on_error": None,
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
        self._runner = create_task(self.run_worker())

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

    async def run_worker(self):
        on_start = self._params.get("on_start")
        if on_start:
            await on_start()

        logger.info("Worker started")
        task_iter: AsyncIterator[TRunArgs] = await self._backend.subscribe()
        sem, tasks, finish_task = self._sem, self._tasks, self.finish_task
        async for task_msg in task_iter:
            fn, args, kwargs, params = task_msg
            params = dict(self._task_params, **params)
            task = create_task(
                self.run_task(fn, args, kwargs, **params), name=fn.__qualname__
            )
            task.add_done_callback(finish_task)
            tasks.add(task)
            logger.info("Run: '%s' (%d)", fn.__qualname__, id(task))
            if sem:
                await sem.acquire()

    async def run_task(
        self,
        func: Callable,
        args: Iterable,
        kwargs: Dict,
        timeout: Number = None,
        delay: Number = None,
        **params,
    ):
        if iscoroutinefunction(func):
            corofunc = func

        else:

            async def corofunc(*args, **kwargs):
                return func(*args, **kwargs)

        # Process delay
        if delay:
            await sleep(cast(float, delay))

        # Process timeout
        if timeout:
            async with async_timeout(cast(float, timeout)):
                return corofunc(*args, **kwargs)

        return await corofunc(*args, **kwargs)

    def finish_task(self, task: Task):
        try:
            exc = task.exception()
            if exc:
                logger.exception(
                    "Fail: '%s' (%d)", task.get_name(), id(task), exc_info=exc
                )
                if self.on_error:
                    create_task(self.on_error(exc))
        except CancelledError:
            pass

        self._tasks.remove(task)
        if self._sem:
            self._sem.release()
        logger.info("Done: '%s' (%d)", task.get_name(), id(task))

    async def join(self):
        """Wait for all tasks to complete."""
        tasks = self._tasks
        if tasks:
            logger.info("Waiting for %d tasks to complete", len(self._tasks))
            return await gather(*tasks, return_exceptions=True)
