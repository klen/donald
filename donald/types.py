from __future__ import annotations

from numbers import Number
from typing import Any, Callable, Coroutine, Literal, Optional, TypedDict, TypeVar

TWorkerOnFn = Callable[[], Coroutine]
TVWorkerOnFn = TypeVar("TVWorkerOnFn", bound=TWorkerOnFn)
TWorkerOnErrFn = Callable[[BaseException], Coroutine]
TVWorkerOnErrFn = TypeVar("TVWorkerOnErrFn", bound=TWorkerOnErrFn)

TV = TypeVar("TV", bound=Any)
TAsyncFn = Callable[..., Coroutine]
TVAsyncFn = TypeVar("TVAsyncFn", bound=TAsyncFn)

TBackendType = Literal["memory", "redis", "amqp"]

TTaskParams = TypedDict(
    "TTaskParams",
    {
        "bind": Optional[bool],
        "delay": Optional[Number],
        "timeout": Optional[Number],
    },
)

TRunArgs = tuple[str, tuple, dict[str, Any], TTaskParams]


class TWorkerParams(TypedDict):
    max_tasks: Optional[int]
    task_defaults: Optional[TTaskParams]
    on_start: Optional[TWorkerOnFn]
    on_stop: Optional[TWorkerOnFn]
    on_error: Optional[TWorkerOnErrFn]
    show_banner: Optional[bool]


class TManagerParams(TypedDict):
    backend: TBackendType
    backend_params: dict
    log_config: dict | None
    log_level: str
    worker_params: TWorkerParams
