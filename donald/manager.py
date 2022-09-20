from __future__ import annotations

from logging.config import dictConfig
from typing import Callable, Dict, Tuple, TypeVar, cast, overload

from . import logger
from .backend import BACKENDS, BaseBackend
from .types import TManagerParams, TTaskParams, TWorkerParams
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

    def __init__(self, **params):
        self._params: TManagerParams = self.defaults
        self.is_started = False
        self.setup(**params)
        self.scheduler = Scheduler()

    def __repr__(self):
        return f"<Donald {self._params['backend']}>"

    def setup(self, **params):
        """Setup the manager."""
        if self.is_started:
            raise RuntimeError("Manager is already started")

        self._params: TManagerParams = dict(self._params, **params)
        self._backend = BACKENDS[self._params["backend"]](
            self._params["backend_params"]
        )

        logger.setLevel(self._params["log_level"].upper())
        if self._params["log_config"]:
            dictConfig(self.params["log_config"])

    async def start(self):
        """Start the manager."""
        logger.info("Starting manager")
        await self._backend.connect()
        self.is_started = True

    async def stop(self):
        logger.info("Stopping manager")
        await self._backend.disconnect()
        self.is_started = False
        logger.info("Manager stopped")

    def create_worker(self, **params):
        """Create a worker."""
        worker_params: TWorkerParams = dict(self._params["worker_params"], **params)
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
    def task(self, **params) -> TWrapper:
        ...

    @overload
    def task(self, fn: Callable) -> TaskWrapper:
        ...

    def task(self, fn: Callable = None, **params):
        """Decorator to wrap a function into a Task object."""

        def wrapper(fn: Callable) -> TaskWrapper:
            return TaskWrapper(self, fn, cast(TTaskParams, params))

        if fn is None:
            return wrapper

        return wrapper(fn)

    def submit(
        self, tw: TaskWrapper, args: Tuple, kwargs: Dict, params: TTaskParams
    ) -> TaskResult:
        """Submit a task to the backend."""
        if not self.is_started:
            raise RuntimeError("Manager is not started")

        return TaskResult(self._backend, tw, args, kwargs, params)

    def on_start(self, fn: Callable):
        """Register a function to be called on worker start."""
        self._params["worker_params"]["on_start"] = fn
        return fn

    def on_stop(self, fn: Callable):
        """Register a function to be called on worker stop."""
        self._params["worker_params"]["on_stop"] = fn
        return fn

    def on_error(self, fn: Callable):
        """Register a function to be called on worker error."""
        self._params["worker_params"]["on_error"] = fn
        return fn


from .scheduler import Scheduler, TInterval
from .tasks import TaskResult, TaskWrapper

TWrapper = TypeVar("TWrapper", bound=Callable[[Callable], TaskWrapper])
