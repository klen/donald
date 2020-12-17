"""Do the work."""

from functools import partial
from queue import Empty
import asyncio as aio
import signal
import sys

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

    def run(self):
        """Wait for a command and do the job."""
        logger.setLevel(self.params['loglevel'].upper())
        loop = aio.events.new_event_loop()
        aio.events.set_event_loop(loop)

        def stop():
            self.running = False

        loop.add_signal_handler(signal.SIGINT, stop)
        loop.run_until_complete(self.runner())

    async def runner(self):
        """Listen for tasks and run."""
        logger.info('Start worker: loop %s', id(aio.get_event_loop()))
        await self.handle('on_start')
        self.running = True

        while self.running:

            if self.tasks < self.params['max_tasks_per_worker']:
                try:
                    message = self.rx.get(block=False)
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
        try:
            res = task.result()
        except BaseException as exc:
            res = exc
            if self.params['on_exception']:
                aio.create_task(self.handle('on_exception', res, sys.exc_info()))

        finally:
            self.tasks -= 1

        self.tx.put((ident, res))

    async def handle(self, etype, *args, **kwargs):
        """Run handlers."""
        for handler in self.params.get(etype, []):
            try:
                await create_task(handler, args, kwargs)
            except Exception as exc:
                logger.error("Error: %s", etype.upper())
                logger.exception(exc)
