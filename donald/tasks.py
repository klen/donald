from __future__ import annotations

from asyncio import create_task
from typing import Callable, Dict, Tuple, cast

from .backend import BaseBackend, current_backend
from .types import TRunArgs, TTaskParams
from .utils import to_coroutinefn


class TaskWrapper:
    """Wrap a given function into a Task object."""

    __slots__ = ("_manager", "_fn", "_params")

    def __init__(self, manager: Donald, fn: Callable, params: TTaskParams):
        if "<locals>" in fn.__qualname__:
            raise ValueError("Cannot use local functions as tasks")

        self._manager = manager
        self._fn = to_coroutinefn(fn)
        self._params: TTaskParams = params

    def __repr__(self):
        return f"<TaskWrapper {self._fn.__qualname__}>"

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    def import_path(self):
        return f"{self._fn.__module__}.{self._fn.__qualname__}"

    def submit(self, *args, **kwargs):
        return self.apply_submit(*args, kwargs=kwargs)

    def apply_submit(self, *args, kwargs: Dict = {}, **params):
        task_params = cast(TTaskParams, dict(self._params, **params))
        return self._manager.submit(self, args, kwargs, task_params)


class TaskResult:
    """Wrap a given function into a Task object."""

    __slots__ = (
        "retries",
        "_data",
        "_submit",
    )

    def __init__(
        self,
        backend: BaseBackend,
        tw: TaskWrapper,
        args: Tuple,
        kwargs: Dict,
        params: TTaskParams,
    ):
        if params.get("bind"):
            args = (self,) + args

        self.retries = 0
        self._data: TRunArgs = (tw, args, kwargs, params)
        self._submit = create_task(backend.submit(self._data))

    def retry(self):
        backend = current_backend.value
        self.retries += 1
        self._submit = create_task(backend.submit(self._data))

    def __repr__(self):
        fn = self._data[0]
        return f"<TaskResult {fn.__qualname__}>"

    def __await__(self):
        return self._submit.__await__()


from .manager import Donald
