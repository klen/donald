"""Main Donald class."""

import asyncio
import sys
import atexit
import datetime
import random

from . import logger, AIOFALSE
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

        # AMQP params
        queue=dict(
            exchange='donald',
            queue='donald',
        ),

    )

    def __init__(self, loop=None, **params):
        """Initialize donald parameters."""
        self.params = AttrDict(self.defaults)
        self.params.update(params)

        logger.setLevel(self.params['loglevel'].upper())
        logger.propagate = False

        # Base loop and queue
        self._loop = loop or asyncio.get_event_loop()
        num_threads = 1 if self.params.num_threads < 1 else self.params.num_threads
        self._threads = tuple(AsyncThreadWorker() for _ in range(num_threads))
        self._waiters = {}
        self._schedules = []
        self._closing = False
        self._started = False
        self._lock = FileLock(self.params.filelock)
        self._exc_handlers = []

        self.queue = Queue(self, loop=self._loop, **self.params.queue)
        self.params.always_eager = self.params.always_eager or not len(self._threads)

    def start(self, loop=None):
        """Start workers.

        :returns: A coroutine
        """
        logger.warning('Start Donald')
        if loop is not None:
            self._loop = self.queue._loop = loop

        if self.params.filelock:
            try:
                self._lock.acquire()
            except FileLocked:
                logger.warning('Donald is locked. Exit.')
                return AIOFALSE

        self._closing = False
        atexit.register(self.stop)

        def start_worker(w):
            return self.future(w.start())

        self._started = True
        return asyncio.wait(map(start_worker, self._threads))

    def stop(self, *args):
        """Stop workers. Disconnect from queue. Cancel schedules.

        The method could be called syncroniously too.

        :returns: A future
        """
        # Stop queue
        if self.is_closed():
            return AIOFALSE

        tasks = [asyncio.ensure_future(self.queue.stop(), loop=self._loop)]

        if self._closing or not self._started:
            return asyncio.wait(tasks, loop=self._loop)

        logger.warning('Stop Donald.')

        if self.params.filelock:
            self._lock.release()

        for task in self._schedules:
            task.cancel()

        # Stop workers
        tasks += [
            asyncio.ensure_future(self.future(t.stop()), loop=self._loop)
            for t in self._threads]

        self._closing = True

        return asyncio.wait(tasks, loop=self._loop)

    def future(self, fut):
        """Just a shortcut on asyncio.wrap_future."""
        return asyncio.wrap_future(fut, loop=self._loop)

    def submit(self, func, *args, **kwargs):
        """Submit given function/coroutine."""
        if self.params.always_eager:
            future = call_with_loop(self._loop, func, *args, **kwargs)
            return self.future(future)

        return asyncio.ensure_future(self._submit(func, *args, **kwargs), loop=self._loop)

    @asyncio.coroutine
    def _submit(self, func, *args, **kwargs):
        """Wait for free worker and submit to it."""
        if len(self._waiters) == len(self._threads):
            yield from asyncio.wait(
                self._waiters, loop=self._loop, return_when=asyncio.FIRST_COMPLETED)
        worker = random.choice(self._threads)
        future, waiter = map(self.future, worker.submit(func, *args, **kwargs))
        self._waiters[id(worker)] = waiter
        waiter.add_done_callback(lambda f: self._waiters.pop(id(worker), None))
        return (yield from future)

    def schedule(self, interval, func, *args, **kwargs):
        """Run given func/coro periodically."""
        if isinstance(interval, datetime.timedelta):
            interval = interval.total_seconds()

        if not isinstance(interval, float):
            interval = float(interval)

        @asyncio.coroutine
        def scheduler():
            while self.is_running():
                try:
                    yield from self.submit(func, *args, **kwargs)
                except Exception as exc:  # noqa
                    self.handle_exc(sys.exc_info(), exc, func, *args, **kwargs)
                yield from asyncio.sleep(interval, loop=self._loop)

        logger.info('Schedule %r', func)
        self._schedules.append(asyncio.ensure_future(scheduler(), loop=self._loop))

    def handle_exc(self, info, exc, func, *args, **kwargs):
        """Handle exception for periodic tasks."""
        info = sys.exc_info()
        logger.error('Unhandled exception for %r', func, exc_info=info)
        for handler in self._exc_handlers:
            call_with_loop(self._loop, handler, info, exc, *args, **kwargs)

    def __str__(self):
        """String representation."""
        return 'Donald [%s]' % self.params['num_threads']
