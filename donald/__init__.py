"""Donald tasks' manager for Asyncio."""

import logging


logger = logging.getLogger(__name__)
handle = logging.StreamHandler()
handle.setFormatter(logging.Formatter(
    '[%(asctime)-15s] [%(process)d] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
logger.addHandler(handle)


from .core import Donald, CronTab as crontab, run_donald # noqa


__version__ = "0.12.0"
__project__ = "Donald"
__author__ = "Kirill Klenov <horneds@gmail.com>"
__license__ = "BSD"
