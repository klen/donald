import os
from concurrent.futures import Future


class AsyncMixin:

    def is_running(self):
        return self._loop and self._loop.is_running()

    def is_closed(self):
        return not self._loop or self._loop.is_closed()

    @property
    def loop(self):
        return self._loop


class AttrDict(dict):

    def __init__(self, *args, **kw):
        self.__dict__ = self
        super(AttrDict, self).__init__(*args, **kw)


class CallableFuture(Future):

    def __init__(self, func, *args, **kw):
        super(CallableFuture, self).__init__()
        self._func = func
        self._args = args
        self._kwargs = kw

    def __call__(self):
        with self._condition:
            try:
                self.set_result(self._func(*self._args, **self._kwargs))
            except Exception as exc:
                self.set_exception(exc)


class Singleton(type):

    instance = None

    def __call__(cls, *args, **kw):
        if not cls.instance:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance


class FileLocked(Exception):
    pass


class FileLock(object):

    """Simplest filelock implementation."""

    def __init__(self, fname, timeout=None, force=False):
        self.fname = fname
        self.fh = None
        self.flags = os.O_CREAT | os.O_RDWR
        for flag in ('O_EXLOCK', 'O_NOINHERIT'):
            self.flags |= getattr(os, flag, 0)

    def acquire(self):
        if os.path.exists(self.fname):
            raise FileLocked()
        self.fh = os.open(self.fname, self.flags)
        os.write(self.fh, bytes(os.getpid()))

    def release(self):
        if self.fh:
            os.close(self.fh)
        os.remove(self.fname)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        self.release()
