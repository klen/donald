"""Tasks' manager for Asyncio."""

__version__ = "0.31.9"
__project__ = "Donald"
__author__ = "Kirill Klenov <horneds@gmail.com>"
__license__ = "BSD"

import logging

logger = logging.getLogger(__name__)

from threading import local

current_manager = local()
current_manager.value = None

from .manager import Donald  # noqa
from .tasks import TaskRun, TaskWrapper  # noqa
