"""Main Donald class."""

import asyncio
import atexit
import datetime

from . import logger
from .queue import Queue
from .utils import AsyncMixin, AttrDict, Singleton, FileLock, FileLocked
from .worker import AsyncThreadWorker, call_with_loop


class Donald(AsyncMixin, metaclass=Singleton):

    """I'am on Donald."""

    defaults = dict(

        # Run tasks imediatelly
        always_eager=False,

        # Number of workers
        num_threads=4,

        # Ensure that the Donald starts only once
        filelock=None,

        # logging level
        loglevel='INFO',

    )

    def __init__(self, loop=None, **params):
        """Initialize donald parameters."""
        self.params = AttrDict(self.defaults)
        self.params.update(params)

        logger.setLevel(self.params['loglevel'].upper())

        # Base loop and queue
        self._loop = loop or asyncio.get_event_loop()
        num_threads = 1 if self.params.num_threads < 1 else self.params.num_threads
        self._threads = tuple(AsyncThreadWorker() for _ in range(num_threads))
        self._tqueue = asyncio.Queue(maxsize=len(self._threads), loop=self._loop)
        self._schedules = []
        self._closing = False
        self._lock = FileLock(self.params.filelock)

        self.queue = Queue(self, loop=self._loop)
        self.params.always_eager = self.params.always_eager or not len(self._threads)

    def start(self):
        """Start workers.

        :returns: A coroutine
        """
        logger.warn('Start Donald')
        if self.params.filelock:
            try:
                self._lock.aquire()
            except FileLocked:
                logger.warn('Donald is locked. Exit.')
                return False

        atexit.register(self.stop)

        def closure(thread):
            fut = asyncio.wrap_future(thread.start(), loop=self._loop)
            fut.add_done_callback(lambda _: self._tqueue.put_nowait(thread))
            return fut

        self.closing = False
        return asyncio.wait(map(closure, self._threads))

    def stop(self):
        """Stop workers. Disconnect from queue. Cancel schedules.

        The method could be called syncroniously too.

        :returns: A future
        """
        if self.is_closed() or self.closing:
            return False

        logger.warn('Stop Donald.')

        for task in self._schedules:
            task.cancel()

        # Stop queue
        tasks = [asyncio.ensure_future(self.queue.stop())]

        # Stop workers
        tasks += [
            asyncio.ensure_future(asyncio.wrap_future(t.stop(), loop=self.loop))
            for t in self._threads]

        self.closing = True

        if self.params.filelock:
            self._lock.release()

        return asyncio.wait([self.queue.stop()])

    def submit(self, func, *args, **kwargs):
        """Submit given function/coroutine."""
        if self.params.always_eager:
            future = call_with_loop(self._loop, func, *args, **kwargs)
            return asyncio.wrap_future(future, loop=self._loop)

        return asyncio.ensure_future(self._submit(func, *args, **kwargs))

    @asyncio.coroutine
    def _submit(self, func, *args, **kwargs):
        """Wait for free worker and submit to it."""
        worker = yield from self._tqueue.get()
        future, waiter = worker.submit(func, *args, **kwargs)
        waiter = asyncio.wrap_future(waiter, loop=self._loop)
        waiter.add_done_callback(lambda f: self._tqueue.put_nowait(worker))
        return (yield from asyncio.wrap_future(future, loop=self._loop))

    def schedule(self, interval, func, *args, **kwargs):
        """Run given func/coro periodically."""
        if isinstance(interval, datetime.timedelta):
            interval = interval.total_seconds()

        if not isinstance(interval, float):
            interval = float(interval)

        @asyncio.coroutine
        def scheduler():
            while self.is_running():
                self.submit(func, *args, **kwargs)
                yield from asyncio.sleep(interval, loop=self._loop)

        logger.info('Schedule %r' % func)
        self._schedules.append(asyncio.ensure_future(scheduler(), loop=self._loop))

    def __str__(self):
        return 'Donald [%s]' % self.params['num_threads']
