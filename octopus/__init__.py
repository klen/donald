import logging


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


from .core import Octopus # noqa
