from __future__ import annotations

from numbers import Number
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Coroutine, Dict, Literal, Optional,
                    Tuple, TypedDict)

if TYPE_CHECKING:
    from .tasks import TaskWrapper

TBackendType = Literal["memory", "redis", "amqp"]

TTaskParams = TypedDict(
    "TTaskParams",
    {
        "bind": Optional[bool],
        "delay": Optional[Number],
        "timeout": Optional[Number],
    },
)

TRunArgs = Tuple["TaskWrapper", Tuple, Dict[str, Any], TTaskParams]

TWorkerParams = TypedDict(
    "TWorkerParams",
    {
        "max_tasks": Optional[int],
        "task_defaults": Optional[TTaskParams],
        "on_start": Optional[Callable[[], Awaitable]],
        "on_stop": Optional[Callable[[], Awaitable]],
        "on_error": Optional[Callable[[BaseException], Coroutine]],
        "show_banner": Optional[bool],
        # "timeout": Number,
        # "task_retry": int,
        # "task_retry_delay": Number,
    },
)

TManagerParams = TypedDict(
    "TManagerParams",
    {
        "backend": TBackendType,
        "backend_params": Dict,
        "log_config": Optional[Dict],
        "log_level": str,
        "worker_params": TWorkerParams,
    },
)
