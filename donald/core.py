"""Main Donald class."""

import asyncio
import atexit
import datetime
import random
import sys

from crontab import CronTab

from . import logger, AIOFALSE, AIOTRUE
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
        self._waiters = set()
        self._schedules = []
        self._closing = False
        self._started = False
        self._lock = FileLock(self.params.filelock)
        self._exc_handlers = []
        self._runner = None

        self.queue = Queue(self, loop=self._loop, **self.params.queue)
        self.params.always_eager = self.params.always_eager or not len(self._threads)

    def init_loop(self, loop):
        """Bind to given loop."""
        if loop and not self._started:
            self._loop = self.queue._loop = loop

    def start(self, loop=None):
        """Start workers.

        :returns: A coroutine
        """
        logger.info('Start Donald')
        self.init_loop(loop)

        if self.params.always_eager:
            return AIOTRUE

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
        if self.is_closed():
            return AIOFALSE

        # Stop queue
        tasks = [asyncio.ensure_future(self.queue.stop(), loop=self._loop)]

        if self._closing or not self._started:
            return asyncio.wait(tasks, loop=self._loop)

        logger.warning('Stop Donald.')

        if self.params.filelock:
            self._lock.release()

        # Stop schedules
        for task in self._schedules:
            task.cancel()

        # Stop runner if exists
        self._runner and self._runner.cancel()

        # Stop workers
        tasks += [
            asyncio.ensure_future(self.future(t.stop()), loop=self._loop)
            for t in self._threads]

        self._closing = True

        return asyncio.wait(tasks, loop=self._loop)

    @asyncio.coroutine
    def run(self, sleep=1):
        """Run the Donald untill stopped."""
        @asyncio.coroutine
        def runner():
            while self.is_running():
                logger.info('Donald is running.')
                yield from asyncio.sleep(sleep, loop=self._loop)

        self._runner = asyncio.ensure_future(runner(), loop=self._loop)
        return self._runner

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
        workers = self._threads
        if len(self._waiters) == len(self._threads):
            logger.info('Wait for worker.')
            _, self._waiters = yield from asyncio.wait(
                self._waiters, loop=self._loop, return_when=asyncio.FIRST_COMPLETED)
            waiters = set(w.worker for w in self._waiters)
            workers = [t for t in self._threads if id(t) not in waiters]

        worker = random.choice(workers)
        future, waiter = map(self.future, worker.submit(func, *args, **kwargs))
        waiter.worker = id(worker)
        self._waiters.add(waiter)
        return (yield from future)

    def schedule(self, interval, func, *args, **kwargs):
        """Run given func/coro periodically."""
        if isinstance(interval, datetime.timedelta):
            timer = interval.total_seconds

        elif isinstance(interval, CronTab):
            timer = lambda: interval.next(default_utc=True)

        elif not isinstance(interval, float):
            timer = lambda: float(interval) # noqa

        @asyncio.coroutine
        def scheduler():
            while self.is_running():
                sleep = timer()
                logger.info('Next %r in %d s', func.__name__, sleep)
                yield from asyncio.sleep(sleep, loop=self._loop)
                try:
                    yield from self.submit(func, *args, **kwargs)
                except Exception as exc:  # noqa
                    self.handle_exc(sys.exc_info(), exc, func, *args, **kwargs)

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
