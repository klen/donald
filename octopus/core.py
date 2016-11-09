"""Main Octopus class."""

import asyncio
import atexit
import datetime

from . import logger
from .queue import Queue
from .utils import AsyncMixin, AttrDict
from .worker import AsyncThreadWorker, call_with_loop


class Octopus(AsyncMixin):

    """I'am on octopus."""

    defaults = dict(

        # Run tasks imediatelly
        always_eager=False,

        # Number of workers
        num_threads=4,

        # logging level
        loglevel='INFO',

    )

    def __init__(self, loop=None, **params):
        """Initialize octopus parameters."""
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

        self.queue = Queue(self, loop=self._loop)
        self.params.always_eager = self.params.always_eager or not len(self._threads)

    def start(self):
        """Start workers.

        :returns: A coroutine
        """
        logger.warn('Start Octopus')

        atexit.register(self.stop)

        futures = []
        for t in self._threads:
            f = asyncio.wrap_future(t.start(), loop=self._loop)
            f.add_done_callback(lambda f: self._tqueue.put_nowait(t))
            futures.append(f)

        self.closing = False
        return asyncio.wait(futures)

    def stop(self):
        """Stop workers. Disconnect from queue. Cancel schedules.

        The method could be called syncroniously too.

        :returns: A future
        """
        if self.is_closed() or self.closing:
            return False

        logger.warn('Stop Octopus.')

        for task in self._schedules:
            task.cancel()

        # Stop queue
        tasks = [asyncio.ensure_future(self.queue.stop())]

        # Stop workers
        tasks += [
            asyncio.ensure_future(asyncio.wrap_future(t.stop(), loop=self.loop))
            for t in self._threads]

        self.closing = True
        return asyncio.wait([self.queue.stop()])

    @asyncio.coroutine
    def submit(self, func, *args, **kwargs):
        """Submit given function/coroutine."""
        if self.params.always_eager:
            future = call_with_loop(self._loop, func, *args, **kwargs)

        else:
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

        async def scheduler():
            while self.is_running():
                await self.submit(func, *args, **kwargs)
                await asyncio.sleep(interval)

        self._schedules.append(asyncio.ensure_future(scheduler(), loop=self._loop))

    def __str__(self):
        return 'Octopus [%s]' % self.params['num_threads']
