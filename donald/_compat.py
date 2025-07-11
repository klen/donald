try:
    from asyncio import timeout as async_timeout  # type: ignore[attr-defined]
except ImportError:  # python 310
    from async_timeout import timeout as async_timeout  # type: ignore[assignment, no-redef]

# python 310
from typing_extensions import Concatenate, NotRequired, Unpack


# ruff: noqa: F401
