"""Donald tasks' manager for Asyncio."""

import logging

logger = logging.getLogger(__name__)
handle = logging.StreamHandler()
handle.setFormatter(
    logging.Formatter(
        "[%(asctime)-15s] [%(process)d] [%(levelname)s] %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
)
logger.addHandler(handle)


from .core import CronTab as crontab
from .core import Donald, run_donald

__all__ = "crontab", "Donald", "run_donald"

__version__ = "0.14.0"
__project__ = "Donald"
__author__ = "Kirill Klenov <horneds@gmail.com>"
__license__ = "BSD"
