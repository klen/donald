"""Do the work."""

import asyncio as aio
import signal
from functools import partial
from queue import Empty
from contextlib import contextmanager

from . import logger
from .utils import repr_func, create_task


def run_worker(rx, tx, params):
    """Create and run a worker inside a process."""
    worker = Worker(rx, tx, params)
    return worker.run()


class Worker:

    """Put a job into a separate process."""

    def __init__(self, rx, tx, params):
        """Initialize the worker."""
        self.rx = rx
        self.tx = tx
        self.params = params
        self.tasks = 0
        self.running = False

        if params['sentry']:
            from sentry_sdk import init

            init(**params['sentry'])

    def run(self):
        """Wait for a command and do the job."""
        logger.setLevel(self.params['loglevel'].upper())
        loop = aio.events.new_event_loop()
        aio.events.set_event_loop(loop)

        def stop():
            self.running = False

        loop.add_signal_handler(signal.SIGINT, stop)
        loop.add_signal_handler(signal.SIGTERM, stop)

        loop.run_until_complete(self.listen())

    async def listen(self):
        """Listen for tasks and run."""
        logger.info('Start worker: loop %s', id(aio.get_event_loop()))
        await self.handle('on_start')
        self.running = True

        rx = self.rx
        params = self.params

        # Mark self as a ready
        self.tx.put(True)

        while self.running:
            if self.tasks < params['max_tasks_per_worker']:
                try:
                    message = rx.get(block=False)
                    if message is None:
                        self.running = False
                        break

                    ident, func, args, kwargs = message
                    logger.info("Run: %s", repr_func(func, args, kwargs))
                    task = create_task(func, args, kwargs)
                    task.add_done_callback(partial(self.done, ident))
                    self.tasks += 1

                except Empty:
                    pass

            await aio.sleep(1e-2)

        # Stop the runner
        logger.info('Stop worker')
        await self.handle('on_stop')

        tasks = [task for task in aio.all_tasks() if task is not aio.current_task()]
        if tasks:
            await aio.sleep(1)

        for task in tasks:
            task.cancel()

    def done(self, ident, task):
        """Send the task's result back to main."""
        with catch_exc(self):
            res = task.exception()
            if res is not None:
                raise res

            res = task.result()

        self.tasks -= 1
        self.tx.put((ident, res))

    async def handle(self, etype):
        """Run handlers."""
        for (func, args, kwargs) in self.params.get(etype, []):
            with catch_exc(self):
                await create_task(func, args, kwargs)


@contextmanager
def catch_exc(worker: Worker):
    """Catch exceptions."""
    try:
        yield worker
    except BaseException as exc:
        logger.exception(exc)
        if worker.params['sentry']:
            from sentry_sdk import capture_exception

            capture_exception(exc)
