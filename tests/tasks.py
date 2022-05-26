import asyncio as aio
import time

from donald import logger

APP_STATUS = None


def blocking(num, **kwargs):
    time.sleep(0.1)
    return num


async def async_blocking(num):
    time.sleep(0.1)
    return num


async def async_(num):
    await aio.sleep(0.1)
    return num


async def async_wait(time):
    await aio.sleep(time)
    return "done"


def exception():
    raise Exception("Test exception.")


async def app_start():
    global APP_STATUS
    await aio.sleep(0.1)
    APP_STATUS = "STARTED"


async def app_stop():
    logger.info("STOP APP")
    global APP_STATUS
    raise Exception
    await aio.sleep(0.1)
    APP_STATUS = "STOPPED"


async def get_app_status():
    return APP_STATUS


async def run_separate_task():
    coros = [async_(num) for num in range(10)]
    return await aio.gather(*coros)


async def write_file(filepath, content):
    with open(filepath, "w") as f:
        f.write(content)
