from __future__ import annotations

from typing import TYPE_CHECKING, Callable, cast

from .types import TRunArgs, TTaskParams
from .utils import current_manager, to_coroutinefn

if TYPE_CHECKING:
    from .manager import Donald


class TaskWrapper:
    """Wrap a given function into a Task object."""

    __slots__ = ("_manager", "_fn", "_params", "_timer")

    def __init__(self, manager: Donald, fn: Callable, params: TTaskParams):
        assert "<locals>" not in fn.__qualname__, "Can't use local functions as tasks"

        self._manager = manager
        self._fn = to_coroutinefn(fn)
        self._params: TTaskParams = params
        self._timer = None

    def __repr__(self):
        return f"<TaskWrapper {self._fn.__qualname__}>"

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    def import_path(self) -> str:
        return f"{self._fn.__module__}.{self._fn.__qualname__}"

    def submit(self, *args, **kwargs):
        return self.apply_submit(*args, kwargs=kwargs)

    def apply_submit(self, *args, kwargs: dict | None = None, **params):
        task_params = cast(TTaskParams, dict(self._params, **params))
        res = TaskRun(self.import_path(), args, kwargs or {}, task_params)
        return self._manager.submit(res)


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
