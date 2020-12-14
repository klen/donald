"""Utils."""

import os
import asyncio as aio
from inspect import iscoroutinefunction


class AsyncMixin:

    """Working with an event loop."""

    def is_running(self):
        """Is the related loop working."""
        return self._loop and self._loop.is_running()

    def is_closed(self):
        """Is the related loop closed."""
        return not self._loop or self._loop.is_closed()

    @property
    def loop(self):
        """Get the binded loop."""
        return self._loop


class AttrDict(dict):

    """Attributes dictionary."""

    def __init__(self, *args, **kw):
        """Do the magic."""
        self.__dict__ = self
        super(AttrDict, self).__init__(*args, **kw)


class FileLock(object):

    """Simplest filelock implementation."""

    class Error(Exception):

        """The lock's error."""

        pass

    def __init__(self, fname, timeout=None, force=False):
        """Initialize the lock."""
        self.fname = fname
        self.fh = None
        self.flags = os.O_CREAT | os.O_RDWR
        for flag in ('O_EXLOCK', 'O_NOINHERIT'):
            self.flags |= getattr(os, flag, 0)

    def acquire(self):
        """Acquire the lock."""
        if os.path.exists(self.fname):
            raise self.Error()
        self.fh = os.open(self.fname, self.flags)
        os.write(self.fh, bytes(os.getpid()))

    def release(self):
        """Release the lock."""
        if self.fh:
            os.close(self.fh)
        os.remove(self.fname)

    def __enter__(self):
        """Enter in the context."""
        self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        """Exit from the context."""
        self.release()


def repr_func(func, args=[], kwargs={}):
    """Stringify the given function with the args."""
    return "%s(%s%s%s)" % (
        func.__qualname__,
        ",".join(map(repr, args)),
        kwargs and ", " or "",
        ",".join("%s=%r" % item for item in kwargs.items())
    )


def create_task(func, args, kwargs):
    """Create a task from the given function."""
    if iscoroutinefunction(func):
        corofunc = func

    else:
        async def corofunc(*args, **kwargs):
            return func(*args, **kwargs)

    return aio.create_task(corofunc(*args, **kwargs))
