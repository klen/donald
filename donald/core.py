"""Donald Asyncio Tasks."""

import asyncio as aio
import datetime
import multiprocessing as mp
import signal
from queue import Empty
from functools import wraps

from crontab import CronTab

from . import logger
from .queue import Queue
from .utils import AsyncMixin, AttrDict, FileLock, repr_func, create_task
from .worker import run_worker


class Donald(AsyncMixin):

    """I'am on Donald."""

    defaults = dict(

        # Run tasks imediatelly in the same process/thread
        fake_mode=False,

        # Number of workers
        num_workers=mp.cpu_count() - 1,

        # Maximum concurent tasks per worker
        max_tasks_per_worker=100,

        # Ensure that the Donald starts only once
        filelock=None,

        # logging level
        loglevel='INFO',

        # Start handlers
        on_start=[],

        # Stop handlers
        on_stop=[],

        # Exception handlers
        on_exception=[],

        # AMQP params
        queue=dict(
            exchange='donald',
            queue='donald',
        ),

    )

    crontab = CronTab

    def __init__(self, on_start=None, on_stop=None, **params):
        """Initialize donald parameters."""
        self.params = AttrDict(self.defaults)
        self.params.update(params)

        logger.setLevel(self.params['loglevel'].upper())
        logger.propagate = False

        self.rx = self.tx = None

        self.workers = None
        self.listener = None
        self.lock = FileLock(self.params.filelock)
        self.schedules = []
        self.waiting = {}
        self._loop = None
        self._started = False

        self.queue = Queue(self, **self.params.queue)

    def __str__(self):
        """Representate as a string."""
        return 'Donald [%s]' % self.params['num_workers']

    def init_loop(self, loop):
        """Bind to given loop."""
        if not self._started:
            self._loop = loop or aio.get_event_loop()
            self.queue.init_loop(loop)

    def is_main(self):
        """Check that we are inside the main process."""
        return mp.current_process().name == 'MainProcess'

    async def start(self, loop=None):
        """Start workers.

        :returns: A coroutine
        """
        logger.warning('Start Donald: loop %s', id(aio.get_event_loop()))
        self.init_loop(loop)

        if self.params.fake_mode:
            return True

        if not self.is_main():
            logger.warning('Donald can be started only inside the main process.')
            return

        if self.params.filelock:
            try:
                self.lock.acquire()
            except self.lock.Error:
                logger.warning('Donald is locked. Exit.')
                return

        ctx = mp.get_context('spawn')
        self.rx = ctx.Queue()
        self.tx = ctx.Queue()

        self.workers = tuple(
            ctx.Process(target=run_worker, args=(self.rx, self.tx, self.params))
            for _ in range(max(self.params.num_workers, 1)))

        # Start listener
        self.listener = aio.create_task(self.listen())

        # Start workers
        for wrk in self.workers:
            wrk.start()

        # Start schedulers
        for idx, schedule in enumerate(self.schedules):
            logger.info('Schedule %s', repr_func(schedule))
            self.schedules[idx] = aio.create_task(schedule())

        # Mark self started
        self._started = True

        return True

    async def stop(self, *args, **kwargs):
        """Stop workers. Disconnect from queue. Cancel schedules.

        The method could be called syncroniously too.

        :returns: A future
        """
        if self.is_closed() or not self._started:
            return

        if not self.is_main():
            logger.warning('Donald can be stopped only inside the main process.')
            return

        logger.warning('Stop Donald')

        if self.params.filelock:
            self.lock.release()

        # Stop runner if exists
        if self.listener:
            self.listener.cancel()

        # Stop schedules
        for task in self.schedules:
            task.cancel()

        # Stop workers
        for wrk in self.workers:
            self.rx.put(None)

        for wrk in self.workers:
            wrk.join(1)
            wrk.terminate()

        self.rx.close()
        self.tx.close()

        self._started = False

        if self.listener and not self.listener.done():
            await aio.sleep(1e-2)

        return True

    async def listen(self):
        """Wait for a result and process."""
        while True:
            if not self.waiting:
                await aio.sleep(1e-2)
                continue

            try:
                ident, res, *args = self.tx.get(block=False)
                fut = self.waiting.pop(ident, None)
                if fut:
                    if isinstance(res, Exception):
                        fut.set_exception(res)
                    else:
                        fut.set_result(res)

            except Empty:
                pass

            await aio.sleep(0)

    def submit(self, func, *args, **kwargs):
        """Submit a task to workers.

        :returns: asyncio.Future
        """
        if not callable(func):
            raise ValueError('Invalid call: %r' % func)

        logger.debug('Submit: %s', repr_func(func, args, kwargs))
        if self.params.fake_mode:
            return create_task(func, args, kwargs)

        if not self._started:
            if self.queue._started:
                return self.queue.submit(func, *args, **kwargs)

            raise RuntimeError('Donald is not started yet')

        fut = self.loop.create_future()
        self.waiting[id(fut)] = fut
        self.rx.put((id(fut), func, args, kwargs))
        return fut

    def schedule(self, interval, *args, **kwargs):
        """Add func to schedules. Use this as a decorator.

        Run given func/coro periodically.
        """
        if callable(interval):
            raise RuntimeError('@donald.schedule(interval) should be used.')

        timer = lambda: float(interval) # noqa

        if isinstance(interval, datetime.timedelta):
            timer = interval.total_seconds

        elif isinstance(interval, CronTab):
            timer = lambda: interval.next(default_utc=True)  # noqa

        def wrapper(func):

            @wraps(func)
            async def scheduler():
                while True:
                    sleep = max(timer(), 0.01)
                    logger.info('Next %s in %0.2f s', repr_func(func, args, kwargs), sleep)
                    await aio.sleep(sleep)
                    self.submit(func, *args, **kwargs)

            self.schedules.append(scheduler)
            return func

        return wrapper

    async def run(self, timer=60):
        """Keep asyncio busy."""
        logger.info('Donald is running')
        while self._started:
            logger.info('Donald is running')
            await aio.sleep(timer)

    def on_start(self, func):
        """Register start handler."""
        self.params.on_start.append(func)
        return func

    def on_stop(self, func):
        """Register stop handler."""
        self.params.on_stop.append(func)
        return func

    def on_exception(self, func):
        """Register exception handler."""
        self.params.on_exception.append(func)
        return func


def run_donald(donald, queue=True):
    """Help to run donald."""
    loop = aio.get_event_loop()

    async def stop_donald():
        if queue:
            await donald.queue.stop()
        await donald.stop()
        loop.stop()

    def handle_signal(loop):
        """Stop donald before exit."""
        loop.remove_signal_handler(signal.SIGTERM)
        loop.remove_signal_handler(signal.SIGINT)
        loop.create_task(stop_donald())

    loop.add_signal_handler(signal.SIGTERM, handle_signal, loop)
    loop.add_signal_handler(signal.SIGINT, handle_signal, loop)
    loop.run_until_complete(donald.start())
    if queue:
        loop.run_until_complete(donald.queue.start())

    try:
        loop.run_forever()
    finally:
        loop.close()
