from __future__ import annotations

import json
import os
from asyncio import Event
from asyncio.tasks import Task, create_task
from logging.config import dictConfig
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Self, cast

from .backend import BACKENDS, BaseBackend
from .utils import ManagerNotReadyError, current_manager, logger
from .worker import Worker

if TYPE_CHECKING:
    from typing import Unpack

    from .types import (
        TInterval,
        TManagerParams,
        TTaskParamsPartial,
        TVWorkerOnErrFn,
        TVWorkerOnFn,
        TWorkerParams,
    )


class Donald:
    """Manage tasks and workers."""

    defaults: ClassVar[TManagerParams] = {
        "log_level": "INFO",
        "log_config": None,
        "backend": "memory",
        "backend_params": {},
        "scheduler_params": {},
        "worker_params": cast("TWorkerParams", {}),
    }

    _backend: BaseBackend

    def __init__(self: Donald, **params):
        self._params: TManagerParams = self.defaults
        self.ready = Event()
        self.setup(**params)
        self.scheduler = Scheduler(**self._params.get("scheduler_params", {}))
        self.submissions: set[Task] = set()
        current_manager.value = self

    def __repr__(self):
        return f"<Donald {self._params['backend']}>"

    @classmethod
    async def create(cls, **params) -> Self:
        """Create a new instance of Donald and start it."""
        self = cls(**params)
        await self.start()
        return self

    def setup(self: Donald, **params):
        """Setup the manager."""
        self._params = cast("TManagerParams", dict(self._params, **params))
        self._backend = BACKENDS[self._params["backend"]](
            self._params["backend_params"],
        )

        logger.setLevel(self._params["log_level"].upper())
        if self._params["log_config"]:
            dictConfig(self._params["log_config"])

    async def start(self):
        """Start the manager."""
        logger.info("Starting tasks manager")
        try:
            await self._backend.connect()
        except Exception as exc:
            logger.exception("Failed to connect backend", exc_info=exc)
            raise
        else:
            self.ready.set()
            logger.info("Tasks manager started")

    async def stop(self):
        logger.info("Stopping tasks manager")
        await self.scheduler.stop()
        try:
            await self._backend.disconnect()
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error while disconnecting backend: {e}")
        finally:
            self.ready.clear()
            logger.info("Tasks manager stopped")

    def create_worker(self: Donald, **params):
        """Create a worker."""
        worker_params = cast(
            "TWorkerParams",
            dict(self._params["worker_params"], **params),
        )
        return Worker(self, worker_params)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    def schedule(
        self, interval: TInterval, *, run_immediately: bool = False
    ) -> Callable[[TaskWrapper], TaskWrapper]:
        """Schedule a task to the backend."""
        return self.scheduler.schedule(interval, run_immediately=run_immediately)

    def task(
        self,
        fn: Any = ...,
        /,
        **params: Unpack[TTaskParamsPartial],
    ) -> Callable[[Callable], TaskWrapper]:
        """Decorator to wrap a function into a Task object."""

        if fn is not ...:
            raise ValueError(
                "Task decorator must be used with parentheses (e.g. @task() or @task(**params))"
            ) from None

        def wrapper(fn: Callable) -> TaskWrapper:
            return TaskWrapper(self, fn, **params)

        return wrapper

    def _assert_run(self, run: TaskRun | Callable, *args, **kwargs) -> TaskRun:
        """Assert that the run is a TaskRun or convert it to one."""
        if isinstance(run, TaskRun):
            return run

        elif callable(run):
            wrapper = TaskWrapper(self, run)
            return wrapper.get_run(*args, kwargs=kwargs)

        else:
            raise TypeError("run must be a TaskRun or a callable function")

    def submit(self, run: TaskRun | Callable, *args, **kwargs) -> Task:
        """Submit a task to the backend."""
        if not self.ready.is_set():
            raise ManagerNotReadyError

        run = self._assert_run(run, *args, **kwargs)
        task = create_task(self._backend.submit(run.data))
        self.submissions.add(task)
        task.add_done_callback(self.submissions.discard)
        return task

    async def submit_and_wait(self, run: TaskRun | Callable, *args, timeout=10, **kwargs) -> Any:
        """Submit a task to the backend and wait for result (RPC)."""
        if not self.ready.is_set():
            raise ManagerNotReadyError

        run = self._assert_run(run, *args, **kwargs)
        return await self._backend.submit_and_wait(run.data, timeout=timeout)

    def on_start(self, fn: TVWorkerOnFn) -> TVWorkerOnFn:
        """Register a function to be called on worker start."""
        self._params["worker_params"]["on_start"] = fn
        return fn

    def on_stop(self, fn: TVWorkerOnFn) -> TVWorkerOnFn:
        """Register a function to be called on worker stop."""
        self._params["worker_params"]["on_stop"] = fn
        return fn

    def on_error(self, fn: TVWorkerOnErrFn) -> TVWorkerOnErrFn:
        """Register a function to be called on worker error."""
        self._params["worker_params"]["on_error"] = fn
        return fn

    async def healthcheck(self, timeout=10) -> bool:
        try:
            result = await self.submit_and_wait(ping, timeout=timeout)
            return result == "pong"
        except Exception as exc:  # noqa: BLE001
            logger.exception("Healthcheck failed", exc_info=exc)
            return False

    async def scheduler_healthcheck(self) -> bool:
        """Check if the scheduler is healthy.

        Reads the scheduler heartbeat file, verifies the PID is alive
        and the last heartbeat timestamp is fresh.
        """
        scheduler = self.scheduler
        try:
            data = json.loads(Path(scheduler.heartbeat_file).read_text())
        except (OSError, json.JSONDecodeError, KeyError):
            return False

        try:
            os.kill(data["pid"], 0)

        except OSError:
            return False

        return (time() - data["timestamp"]) < (scheduler.heartbeat_interval * 2)


async def ping():
    """Ping the manager to check if it's running."""
    return "pong"


from .scheduler import Scheduler  # noqa: E402
from .tasks import TaskRun, TaskWrapper  # noqa: E402
