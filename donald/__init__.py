import logging


logger = logging.getLogger(__name__)
handle = logging.StreamHandler()
handle.setFormatter(logging.Formatter('%(thread)d: %(message)s'))
logger.addHandler(handle)


from .core import Donald # noqa


__version__ = "0.0.5"
__project__ = "Donald"
__author__ = "Kirill Klenov <horneds@gmail.com>"
__license__ = "BSD"
