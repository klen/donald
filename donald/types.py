from __future__ import annotations

from datetime import timedelta
from typing import (
    Any,
    Callable,
    Coroutine,
    Literal,
    Optional,
    TypedDict,
    TypeVar,
    Union,
)

from crontab import CronTab
from typing_extensions import NotRequired  # py310

TWorkerOnFn = Callable[[], Coroutine]
TVWorkerOnFn = TypeVar("TVWorkerOnFn", bound=TWorkerOnFn)
TWorkerOnErrFn = Callable[[BaseException], Coroutine]
TVWorkerOnErrFn = TypeVar("TVWorkerOnErrFn", bound=TWorkerOnErrFn)

TV = TypeVar("TV", bound=Any)
TAsyncFn = Callable[..., Coroutine]
TVAsyncFn = TypeVar("TVAsyncFn", bound=TAsyncFn)

TBackendType = Literal["memory", "redis", "amqp"]

TInterval = Union[timedelta, int, float, str, CronTab]

class TTaskParams(TypedDict):
    bind: bool
    delay: float
    timeout: float
    retries_max: int
    retries_backoff_factor: float
    retries_backoff_max: float
    reply_to: NotRequired[str]
    correlation_id: NotRequired[str]

class TTaskParamsPartial(TypedDict):
    bind: NotRequired[bool]
    delay: NotRequired[float]
    timeout: NotRequired[float]
    retries_max: NotRequired[int]
    retries_backoff_factor: NotRequired[float]
    retries_backoff_max: NotRequired[float]

TRunArgs = tuple[str, tuple, dict[str, Any], TTaskParams]

class TWorkerParams(TypedDict):
    max_tasks: Optional[int]
    on_start: Optional[TWorkerOnFn]
    on_stop: Optional[TWorkerOnFn]
    on_error: Optional[TWorkerOnErrFn]
    show_banner: Optional[bool]
    task_defaults: Optional[TTaskParamsPartial]


class TManagerParams(TypedDict):
    backend: TBackendType
    backend_params: dict
    log_config: dict | None
    log_level: str
    worker_params: TWorkerParams
