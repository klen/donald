from __future__ import annotations

import logging
from asyncio.coroutines import iscoroutinefunction
from functools import wraps
from importlib import import_module
from threading import local
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Final, overload

if TYPE_CHECKING:
    from donald.types import TV, TVAsyncFn


def import_obj(path: str) -> Any:
    """Import an object from a string path."""
    module_path, _, obj_path = path.rpartition(".")
    try:
        module = import_module(module_path)
    except (ImportError, AttributeError, ValueError) as exc:
        raise ImportError(f"Could not import {path!r}") from exc
    else:
        return getattr(module, obj_path)


@overload
def to_coroutinefn(fn: TVAsyncFn) -> TVAsyncFn:
    ...


@overload
def to_coroutinefn(fn: Callable[..., TV]) -> Callable[..., Coroutine[Any, Any, TV]]:
    ...


def to_coroutinefn(fn):
    """Convert a function to a coroutine function."""
    if iscoroutinefunction(fn):
        return fn

    @wraps(fn)
    async def corofn(*args, **kwargs):
        return fn(*args, **kwargs)

    return corofn


class DonaldError(RuntimeError):
    """Donald error."""


class ManagerNotReadyError(DonaldError):
    """Manager is not ready."""


class BackendNotReadyError(DonaldError):
    """Backend is not ready."""


class SchedulerNotReadyError(DonaldError):
    """Scheduler is not ready."""


logger: Final = logging.getLogger("donald")

current_manager: Final = local()
current_manager.value = None
