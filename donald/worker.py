import asyncio
import threading

from . import logger
from .utils import AsyncMixin, CallableFuture, Future


class AsyncThreadWorker(AsyncMixin, threading.Thread):

    def __init__(self, *args, **kwargs):
        super(AsyncThreadWorker, self).__init__(*args, **kwargs)
        self._loop = None

    def start(self):
        self._future = Future()
        super(AsyncThreadWorker, self).start()
        return self._future

    def run(self):
        logger.info('Start loop for worker: %d' % id(self))
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.call_soon_threadsafe(
            asyncio.futures._set_result_unless_cancelled, self._future, True)
        self._loop.run_forever()

    def stop(self):
        if self.is_closed():
            fut = Future()
            fut.set_result(False)
            return fut
        logger.info('Stop loop for worker: %d' % id(self))
        for task in asyncio.Task.all_tasks(loop=self._loop):
            task.cancel()

        fut = CallableFuture(self._loop.stop)
        self._loop.call_soon_threadsafe(fut)
        return fut

    def submit(self, func, *args, **kwargs):
        """Submit function/coroutine to current loop."""
        if not self.is_running():
            raise RuntimeError('Worker loop is stopped.')

        logger.info('Submit task with worker: %d' % id(self))
        job = call_with_loop(self._loop, func, *args, **kwargs)
        waiter = Future()
        self._loop.call_soon(
            asyncio.futures._set_result_unless_cancelled, waiter, True)
        return job, waiter


def call_with_loop(loop, func, *args, **kwargs):
    """Call given coro/function with given loop."""
    if asyncio.iscoroutine(func):
        return asyncio.run_coroutine_threadsafe(func, loop)

    if asyncio.iscoroutinefunction(func):
        return asyncio.run_coroutine_threadsafe(func(*args, **kwargs), loop)

    fut = CallableFuture(func, *args, **kwargs)
    loop.call_soon_threadsafe(fut)
    return fut
