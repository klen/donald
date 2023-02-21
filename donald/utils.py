from asyncio.coroutines import iscoroutinefunction
from functools import wraps
from importlib import import_module
from typing import Any, Callable, Coroutine, Optional, overload

from donald.types import TV, TVAsyncFn


def import_obj(path: str, obj_name: Optional[str] = None) -> Any:
    """Import an object from a string path."""
    module_path, _, obj_path = path.rpartition(".")
    try:
        module = import_module(module_path)
        return getattr(module, obj_path)
    except (ImportError, AttributeError, ValueError) as exc:
        raise ImportError(f"Could not import {path!r}") from exc


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
