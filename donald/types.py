from __future__ import annotations

from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Literal,
    Optional,
    ParamSpec,
    TypedDict,
    TypeVar,
    Union,
)

from crontab import CronTab

if TYPE_CHECKING:
    from typing import NotRequired

P = ParamSpec("P")
R = TypeVar("R")

TWorkerOnFn = Callable[[], Awaitable[Any]]
TVWorkerOnFn = TypeVar("TVWorkerOnFn", bound=TWorkerOnFn)
TWorkerOnErrFn = Callable[[BaseException], Awaitable[Any]]
TVWorkerOnErrFn = TypeVar("TVWorkerOnErrFn", bound=TWorkerOnErrFn)

TV = TypeVar("TV")
TAsyncFn = Callable[..., Awaitable[Any]]
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


TRunArgs = tuple[str, tuple[Any, ...], dict[str, Any], TTaskParams]


class TWorkerParams(TypedDict, total=False):
    max_tasks: Optional[int]
    on_start: Optional[TWorkerOnFn]
    on_stop: Optional[TWorkerOnFn]
    on_error: Optional[TWorkerOnErrFn]
    show_banner: Optional[bool]
    task_defaults: Optional[TTaskParamsPartial]


class TSchedulerParams(TypedDict, total=False):
    heartbeat_interval: float
    heartbeat_file: str


class TManagerParams(TypedDict):
    backend: TBackendType
    backend_params: dict[str, Any]
    log_config: dict[str, Any] | None
    log_level: str
    scheduler_params: TSchedulerParams
    worker_params: TWorkerParams


class TManagerParamsPartial(TypedDict, total=False):
    backend: TBackendType
    backend_params: dict[str, Any]
    log_config: dict[str, Any] | None
    log_level: str
    scheduler_params: TSchedulerParams
    worker_params: TWorkerParams
