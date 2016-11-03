from concurrent.futures import Future


class AsyncMixin:

    def is_running(self):
        return self._loop and self._loop.is_running()

    def is_closed(self):
        return not self._loop or self._loop.is_closed()


class AttrDict(dict):

    def __init__(self, *args, **kwargs):
        self.__dict__ = self
        super(AttrDict, self).__init__(*args, **kwargs)


class CallableFuture(Future):

    def __init__(self, func, *args, **kwargs):
        super(CallableFuture, self).__init__()
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def __call__(self):
        with self._condition:
            try:
                self.set_result(self._func(*self._args, **self._kwargs))
            except Exception as exc:
                self.set_exception(exc)
