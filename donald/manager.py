from __future__ import annotations

from asyncio.tasks import create_task
from logging.config import dictConfig
from typing import Callable, Optional, cast, overload

from . import current_manager, logger
from .backend import BACKENDS, BaseBackend
from .types import TManagerParams, TTaskParams, TVWorkerOnErrFn, TVWorkerOnFn, TWorkerParams
from .worker import Worker


class Donald:
    """Manage tasks and workers."""

    defaults: TManagerParams = {
        "log_level": "INFO",
        "log_config": None,
        "backend": "memory",
        "backend_params": {},
        "worker_params": cast(TWorkerParams, {}),
    }

    _backend: BaseBackend

    def __init__(self: Donald, **params):
        self._params: TManagerParams = self.defaults
        self.is_started = False
        self.setup(**params)
        self.scheduler = Scheduler()
        current_manager.value = self

    def __repr__(self):
        return f"<Donald {self._params['backend']}>"

    def setup(self: Donald, **params):
        """Setup the manager."""
        if self.is_started:
            raise RuntimeError("Manager is already started")

        self._params = cast(TManagerParams, dict(self._params, **params))
        self._backend = BACKENDS[self._params["backend"]](
            self._params["backend_params"]
        )

        logger.setLevel(self._params["log_level"].upper())
        if self._params["log_config"]:
            dictConfig(self._params["log_config"])

    async def start(self):
        """Start the manager."""
        logger.info("Starting tasks manager")
        await self._backend.connect()
        self.is_started = True
        logger.info("Tasks manager started")

    async def stop(self):
        logger.info("Stopping tasks manager")
        await self._backend.disconnect()
        self.is_started = False
        logger.info("Tasks manager stopped")

    def create_worker(self: Donald, **params):
        """Create a worker."""
        worker_params = cast(
            TWorkerParams, dict(self._params["worker_params"], **params)
        )
        return Worker(self._backend, worker_params)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    def schedule(self, interval: TInterval):
        """Schedule a task to the backend."""
        return self.scheduler.schedule(interval)

    @overload
    def task(self, fn: Callable) -> TaskWrapper:
        ...

    @overload
    def task(self, **params) -> Callable[[Callable], TaskWrapper]:
        ...

    def task(self, fn: Optional[Callable] = None, **params):
        """Decorator to wrap a function into a Task object."""

        def wrapper(fn: Callable) -> TaskWrapper:
            return TaskWrapper(self, fn, cast(TTaskParams, params))

        if fn is None:
            return wrapper

        return wrapper(fn)

    def submit(self, run: TaskRun):
        """Submit a task to the backend."""
        if not self.is_started:
            raise RuntimeError("Manager is not started")

        return create_task(self._backend.submit(run.data))

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


from .scheduler import Scheduler, TInterval
from .tasks import TaskRun, TaskWrapper
