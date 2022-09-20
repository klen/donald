from asyncio.coroutines import iscoroutinefunction
from functools import wraps
from importlib import import_module
from pickle import dumps as pickle_dumps
from pickle import loads as pickle_loads
from typing import Any, Callable, Coroutine, TypeVar, cast, overload

from .types import TRunArgs


def import_obj(path: str, obj_name: str = None) -> Any:
    """Import an object from a string path."""
    module_path, _, obj_path = path.rpartition(".")
    try:
        module = import_module(module_path)
        return getattr(module, obj_path)
    except (ImportError, AttributeError, ValueError) as exc:
        raise ImportError(f"Could not import {path!r}") from exc


def dumps(data: TRunArgs) -> bytes:
    """Serialize an object to a string."""
    data = (data[0].import_path(), *data[1:])
    res = pickle_dumps(data)
    return res


def loads(data: bytes) -> TRunArgs:
    """Deserialize an object from a string."""
    obj_path, *params = pickle_loads(data)
    res = (import_obj(obj_path), *params)
    return cast(TRunArgs, res)


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
