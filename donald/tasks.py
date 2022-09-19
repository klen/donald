from __future__ import annotations

from asyncio import create_task
from typing import Callable, Dict, Tuple, cast

from .backend import BaseBackend, current_backend
from .types import TRunArgs, TTaskParams


class TaskWrapper:
    """Wrap a given function into a Task object."""

    def __init__(self, manager: Donald, fn: Callable, params: TTaskParams):
        self._manager = manager
        self._fn = fn
        self._params: TTaskParams = params

    def __repr__(self):
        return f"<TaskWrapper {self._fn.__qualname__}>"

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    def submit(self, *args, **kwargs):
        return self.apply_submit(*args, kwargs=kwargs)

    def apply_submit(self, *args, kwargs: Dict = {}, **params):
        task_params = cast(TTaskParams, dict(self._params, **params))
        return self._manager.submit(self._fn, args, kwargs, task_params)


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
        fn: Callable,
        args: Tuple,
        kwargs: Dict,
        params: TTaskParams,
    ):
        if params.get("bind"):
            args = (self,) + args

        self.retries = 0
        self._data: TRunArgs = (fn, args, kwargs, params)
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
