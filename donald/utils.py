from asyncio.coroutines import iscoroutinefunction
from functools import wraps
from importlib import import_module
from typing import Any, Callable, Coroutine, TypeVar, overload


def import_obj(path: str, obj_name: str = None) -> Any:
    """Import an object from a string path."""
    module_path, _, obj_path = path.rpartition(".")
    try:
        module = import_module(module_path)
        return getattr(module, obj_path)
    except (ImportError, AttributeError, ValueError) as exc:
        raise ImportError(f"Could not import {path!r}") from exc


TRes = TypeVar("TRes")
TCoroFn = TypeVar("TCoroFn", bound=Callable[..., Coroutine])


@overload
def to_coroutinefn(fn: TCoroFn) -> TCoroFn:
    ...


@overload
def to_coroutinefn(fn: Callable[..., TRes]) -> Callable[..., Coroutine[Any, Any, TRes]]:
    ...


def to_coroutinefn(fn):
    """Convert a function to a coroutine function."""
    if iscoroutinefunction(fn):
        return fn

    @wraps(fn)
    async def corofn(*args, **kwargs):
        return fn(*args, **kwargs)

    return corofn
