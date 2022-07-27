"""Donald Asyncio Tasks."""

import asyncio
import datetime
import multiprocessing as mp
import signal
import typing as t
from queue import Empty

from crontab import CronTab

from . import logger
from .queue import Queue
from .utils import AsyncMixin, FileLock, create_task
from .worker import run_worker


class Donald(AsyncMixin):

    """I'am on Donald."""

    defaults: t.Dict[str, t.Any] = {
        # Run tasks imediatelly in the same process/thread
        "fake_mode": False,
        # Number of workers
        "num_workers": mp.cpu_count() - 1,
        # Maximum concurent tasks per worker
        "max_tasks_per_worker": 100,
        # Ensure that the Donald starts only once
        "filelock": None,
        # logging level
        "loglevel": "INFO",
        # Start handlers
        "on_start": [],
        # Stop handlers
        "on_stop": [],
        # AMQP params
        "queue_name": "donald",
        "queue_params": {},
        # Sentry options ({'dsn': '...'})
        "sentry": None,
    }

    crontab = CronTab

    _loop: t.Optional[asyncio.AbstractEventLoop] = None
    _started = False

    rx: t.Optional[mp.Queue] = None
    tx: t.Optional[mp.Queue] = None
    workers: t.Optional[t.Tuple[mp.context.SpawnProcess, ...]] = None
    listener: t.Optional[asyncio.Task] = None

    def __init__(
        self, on_start: t.Callable = None, on_stop: t.Callable = None, **params
    ):
        """Initialize donald parameters."""
        self.params = dict(self.defaults, **params)

        logger.setLevel(self.params["loglevel"].upper())
        logger.propagate = False

        self.lock = FileLock(self.params["filelock"])
        self.schedules: t.List = []
        self.waiting: t.Dict[int, asyncio.Future] = {}

        self.queue = Queue(
            self, **dict(self.params["queue_params"], queue=self.params["queue_name"])
        )

    def __str__(self) -> str:
        """Representate as a string."""
        return f"Donald [{self.params['num_workers']}]"

    async def start(self, loop: asyncio.AbstractEventLoop = None):
        """Start workers.

        :returns: A coroutine
        """
        logger.warning("Start Donald: loop %s", id(asyncio.get_event_loop()))
        self._loop = loop or asyncio.get_event_loop()
        self.queue.init_loop(loop)

        if self.params["fake_mode"]:
            return True

        if self.params["filelock"]:
            try:
                self.lock.acquire()
            except self.lock.Error:
                logger.warning("Donald is locked. Exit.")
                return

        ctx = mp.get_context("spawn")
        self.rx = ctx.Queue()
        self.tx = ctx.Queue()

        self.workers = tuple(
            ctx.Process(target=run_worker, args=(self.rx, self.tx, self.params))
            for _ in range(max(self.params["num_workers"], 1))
        )

        # Start workers
        for wrk in self.workers:
            wrk.start()
            self.tx.get(block=True)

        # Start listener
        self.listener = asyncio.create_task(self.listen())

        # Start schedulers
        for idx, schedule in enumerate(self.schedules):
            logger.info(f"Schedule '{schedule.__qualname__}'")
            self.schedules[idx] = asyncio.create_task(schedule())

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

        logger.warning("Stoping Donald")

        if self.params["filelock"]:
            self.lock.release(silent=True)

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
            await asyncio.sleep(1e-2)

        logger.warning("Donald is stopped")
        return True

    async def __aenter__(self) -> "Donald":
        """Support usage as a context manager."""
        await self.start()
        return self

    async def __aexit__(self, *args):
        """Support usage as a context manager."""
        await self.stop()

    def __submit(
        self, fut: t.Optional[asyncio.Future], func: t.Callable, *args, **kwargs
    ) -> t.Optional[asyncio.Future]:
        """Submit the given task to workers."""
        if not callable(func):
            raise ValueError("Invalid task: %r" % func)

        if not self._started:
            raise RuntimeError("Donald is not started yet")

        logger.debug("Submit: '{func.__qualname__}'")
        ident = None
        if fut:
            ident = id(fut)
            self.waiting[ident] = fut

        # Send task to workers
        rx = t.cast(mp.Queue, self.rx)
        rx.put((ident, func, args, kwargs))
        return fut

    async def listen(self):
        """Wait for a result and process."""
        tx = self.tx
        waiting = self.waiting
        while True:
            if not waiting:
                await asyncio.sleep(1e-2)
                continue

            try:
                ident, res, *args = tx.get(block=False)
                fut = waiting.pop(ident, None)
                if fut:
                    if isinstance(res, Exception):
                        fut.set_exception(res)
                    else:
                        fut.set_result(res)

                # Leave the objects from the closure
                fut = ident = res = None

            except Empty:
                pass

            await asyncio.sleep(0)

    def submit(self, func: t.Callable, *args, **kwargs) -> asyncio.Future:
        """Submit a task to workers."""
        if self.params["fake_mode"]:
            return create_task(func, args, kwargs)

        fut = self.loop.create_future()
        res = self.__submit(fut, func, *args, **kwargs)
        res = t.cast(asyncio.Future, res)
        return res

    def submit_nowait(self, func: t.Callable, *args, **kwargs):
        """Submit a task to workers."""
        if self.params["fake_mode"]:
            create_task(func, args, kwargs)

        else:
            self.__submit(None, func, *args, **kwargs)

    def schedule(
        self,
        interval: t.Union[int, float, datetime.timedelta, CronTab],
        *args,
        **kwargs,
    ):
        """Add func to schedules. Use this as a decorator.

        Run given func/coro periodically.
        """
        if callable(interval):
            raise RuntimeError("@donald.schedule(interval) should be used.")

        if isinstance(interval, str) and " " in interval:
            interval = CronTab(interval)

        if isinstance(interval, datetime.timedelta):
            timer = interval.total_seconds

        elif isinstance(interval, CronTab):
            timer = lambda: interval.next(default_utc=True)  # type: ignore

        else:
            timer = lambda: float(interval)  # type: ignore

        def wrapper(func):
            async def scheduler():
                while True:
                    sleep = max(timer(), 1e-2)
                    logger.info("Next '%s' in %0.2f s", func.__qualname__, sleep)
                    await asyncio.sleep(sleep)
                    self.submit_nowait(func, *args, **kwargs)

            self.schedules.append(scheduler)
            return func

        return wrapper

    async def run(self, tick: int = 60):
        """Keep asyncio busy."""
        while self._started:
            logger.info("Donald is running")
            await asyncio.sleep(tick)

    def on_start(self, func: t.Callable, *args, **kwargs) -> t.Callable:
        """Register start handler."""
        self.params["on_start"].append((func, args, kwargs))
        return func

    def on_stop(self, func: t.Callable, *args, **kwargs) -> t.Callable:
        """Register stop handler."""
        self.params["on_stop"].append((func, args, kwargs))
        return func


def run_donald(donald, queue=True):
    """Help to run donald."""
    loop = asyncio.get_event_loop()

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
