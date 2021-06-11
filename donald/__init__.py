"""Donald tasks' manager for Asyncio."""
import logging
import asyncio


AIOFALSE = asyncio.Future()
AIOFALSE.set_result(False)
AIOTRUE = asyncio.Future()
AIOTRUE.set_result(True)


logger = logging.getLogger(__name__)
handle = logging.StreamHandler()
handle.setFormatter(logging.Formatter(
    '[%(asctime)-15s] [%(process)d] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
logger.addHandler(handle)


from .core import Donald, CronTab as crontab, run_donald # noqa


__version__ = "0.9.1"
__project__ = "Donald"
__author__ = "Kirill Klenov <horneds@gmail.com>"
__license__ = "BSD"
