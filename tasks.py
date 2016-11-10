import time
import asyncio

from donald import Donald, logger


def ping():
    print('Pong %d' % time.time())


@asyncio.coroutine
def task():
    logger.info('Blocking sleep for 3 seconds.')
    time.sleep(3)
    logger.info('Done.')


donald = Donald()
donald.schedule(1, ping)
donald.submit(task)
donald.submit(task)
donald.submit(task)
donald.submit(task)
donald.submit(task)
donald.submit(task)
donald.submit(task)
donald.submit(task)
donald.submit(task)
donald.submit(task)
