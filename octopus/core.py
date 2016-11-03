"""Main Octopus class."""

import asyncio
import atexit
import datetime
import random

from . import logger
from .queue import Queue
from .utils import AsyncMixin, AttrDict
from .worker import AsyncThreadWorker, call_with_loop


class Octopus(AsyncMixin):

    """I'am an octopus."""

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
        self._threads = tuple(AsyncThreadWorker() for _ in range(self.params.num_threads))
        self._schedules = []

        self.queue = Queue(self, loop=self._loop)
        self.params.always_eager = self.params.always_eager or not len(self._threads)

    def start(self):
        """Start workers.

        :returns: A coroutine
        """
        logger.warn('Start Octopus')
        atexit.register(self.stop)
        return asyncio.gather(*[
            asyncio.wrap_future(t.start(), loop=self._loop) for t in self._threads])

    def stop(self):
        """Stop workers. Disconnect from queue

        :returns: A coroutine
        """
        logger.warn('Stop Octopus')
        futures = [self.queue.stop()]
        futures += [asyncio.wrap_future(t.stop(), loop=self._loop) for t in self._threads]

        for task in self._schedules:
            task.cancel()

        return asyncio.gather(*futures)

    def submit(self, func, *args, **kwargs):
        """Submit given function/coroutine."""
        if self.params.always_eager:
            future = call_with_loop(self._loop, func, *args, **kwargs)
        else:
            # TODO: Use better method to choose workers.
            t = random.choice(self._threads)
            future = t.submit(func, *args, **kwargs)

        return asyncio.wrap_future(future, loop=self._loop)

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
