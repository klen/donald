from __future__ import annotations

from asyncio.tasks import Task, create_task
from logging.config import dictConfig
from typing import TYPE_CHECKING, Any, Callable, ClassVar, cast

from typing_extensions import Unpack

from .backend import BACKENDS, BaseBackend
from .utils import ManagerNotReadyError, current_manager, logger
from .worker import Worker

if TYPE_CHECKING:
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
        "worker_params": cast("TWorkerParams", {}),
    }

    _backend: BaseBackend

    def __init__(self: Donald, **params):
        self._params: TManagerParams = self.defaults
        self.is_started = False
        self.setup(**params)
        self.scheduler = Scheduler()
        self.submissions: set[Task] = set()
        current_manager.value = self

    def __repr__(self):
        return f"<Donald {self._params['backend']}>"

    def setup(self: Donald, **params):
        """Setup the manager."""
        assert not self.is_started, "Manager is already started"

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
            "TWorkerParams",
            dict(self._params["worker_params"], **params),
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

    def task(
        self,
        fn: Any = ...,
        /,
        **params: Unpack[TTaskParamsPartial],
    ) -> Callable[[Callable], TaskWrapper]:
        """Decorator to wrap a function into a Task object."""

        if fn is not ...:
            raise ValueError("Task decorator must be used with parentheses (e.g. @task() or @task(**params))") from None  # noqa: E501, TRY003

        def wrapper(fn: Callable) -> TaskWrapper:
            return TaskWrapper(self, fn, **params)

        return wrapper

    def submit(self, run: TaskRun | Callable, *args, **kwargs) -> Task:
        """Submit a task to the backend."""
        if not self.is_started:
            raise ManagerNotReadyError

        if not isinstance(run, TaskRun):
            wrapper = TaskWrapper(self, run)
            run = wrapper.get_run(*args, kwargs=kwargs)

        assert isinstance(run, TaskRun)
        task = create_task(self._backend.submit(run.data))
        self.submissions.add(task)
        task.add_done_callback(self.submissions.discard)
        return task

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


from .scheduler import Scheduler  # noqa: E402
from .tasks import TaskRun, TaskWrapper  # noqa: E402
