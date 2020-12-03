"""Do the work."""

import asyncio as aio
import multiprocessing as mp
from functools import partial
from queue import Empty

from . import logger
from .utils import repr_func, create_task


class ProcessWorker(mp.Process):

    """Put a job into a separate process."""

    def __init__(self, rx, tx, **params):
        """Initialize the worker."""
        super(ProcessWorker, self).__init__()
        self.rx = rx
        self.tx = tx
        self.params = params
        self.started = mp.Event()
        self.stopped = mp.Event()

    def run(self):
        """Wait for a command and do the job."""
        logger.setLevel(self.params['loglevel'].upper())
        aio.run(self.runner())

    async def runner(self):
        """Listen for tasks and run."""
        logger.info('Start worker')
        await self.handle('on_start')
        self.started.set()

        while True:
            if self.stopped.is_set():
                break

            tasks = aio.all_tasks()
            if len(tasks) - 1 >= self.params['max_tasks_per_worker']:
                await aio.sleep(.01)

            try:
                ident, func, args, kwargs = self.rx.get(block=False)
                logger.info("Run: %s", repr_func(func, args, kwargs))
                task = create_task(func, args, kwargs)
                task.add_done_callback(partial(self.done, ident))

            except Empty:
                pass

            await aio.sleep(0)

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

        self.tx.put((ident, res))

    async def handle(self, etype):
        """Run handlers."""
        for handler in self.params.get(etype, []):
            try:
                await create_task(handler, [], {})
            except Exception as exc:
                logger.error("Error: %s", etype.upper())
                logger.exception(exc)
