from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, cast

from typing_extensions import Concatenate, Unpack

from .utils import current_manager, to_coroutinefn

if TYPE_CHECKING:
    from .manager import Donald
    from .types import TInterval, TRunArgs, TTaskParams, TTaskParamsPartial


class TaskWrapper:
    """Wrap a given function into a Task object."""

    __slots__ = ("_failback", "_fn", "_manager", "_params")

    def __init__(  # noqa: PLR0913
        self,
        manager: Donald | None,
        fn: Callable,
        *,
        bind: bool = False,
        delay: float = 0,
        timeout: float = 0,
        retries_max: int = 0,
        retries_backoff_factor: float = 0,
        retries_backoff_max: float = 600,
    ):
        assert "<locals>" not in fn.__qualname__, "Can't use local functions as tasks"

        self._manager = manager
        self._fn = to_coroutinefn(fn)
        self._failback: Callable | None = None
        self._params: TTaskParams = {
            "bind": bind,
            "delay": delay,
            "timeout": timeout,
            "retries_max": retries_max,
            "retries_backoff_max": retries_backoff_max,
            "retries_backoff_factor": retries_backoff_factor,
        }

    def __repr__(self):
        return f"<TaskWrapper {self._fn.__qualname__}>"

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    @staticmethod
    def import_path(fn: Callable) -> str:
        return f"{fn.__module__}.{fn.__qualname__}"

    def get_run(self, *args, kwargs: dict, **params: Unpack[TTaskParamsPartial]) -> TaskRun:
        task_params = cast("TTaskParams", dict(self._params, **params))
        return TaskRun(self.import_path(self._fn), args, kwargs or {}, task_params)

    def submit(self, *args, **kwargs):
        return self.apply_submit(*args, kwargs=kwargs)

    async def submit_and_wait(self, *args, timeout=10, **kwargs):
        manager = self._manager
        if not manager:
            raise RuntimeError("Manager is not set for this task")
        run = self.get_run(*args, kwargs=kwargs or {})
        return await manager.submit_and_wait(run, timeout=timeout)

    def apply_submit(self, *args, kwargs: dict | None = None, **params):
        manager = self._manager
        if not manager:
            raise RuntimeError("Manager is not set for this task")
        run = self.get_run(*args, kwargs=kwargs or {}, **params)
        return manager.submit(run)

    def schedule(
        self, interval: TInterval, *, run_immediately: bool = False, backoff: float = 0
    ) -> Callable[[TaskWrapper], TaskWrapper]:
        manager = self._manager
        if not manager:
            raise RuntimeError("Manager is not set for this task")
        return manager.schedule(interval, run_immediately=run_immediately, backoff=backoff)(self)

    def failback(self):
        def wrapper(fn: Callable[Concatenate[Exception, ...], Any]):
            self._failback = to_coroutinefn(fn)

        return wrapper


class TaskRun:
    """Wrap a given function into a Task object."""

    __slots__ = ("data", "retries")

    def __init__(
        self,
        path: str,
        args: tuple,
        kwargs: dict,
        params: TTaskParams,
    ):
        if params.get("bind"):
            args = (self, *args)

        self.retries = 0
        self.data: TRunArgs = (path, args, kwargs, params)

    def retry(self: TaskRun):
        manager: Donald = current_manager.value
        self.retries += 1
        return manager.submit(self)

    def __repr__(self):
        path = self.data[0]
        return f"<TaskRun {path}>"
