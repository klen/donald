try:
    from asyncio import timeout as async_timeout  # type: ignore[attr-defined]
except ImportError:  # python 310
    from async_timeout import timeout as async_timeout  # type: ignore[assignment, no-redef]


# ruff: noqa: F401
