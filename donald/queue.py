"""AMPQ support."""
import asyncio
import sys
import pickle
import uuid
from importlib import import_module

import aioamqp

from . import logger, AIOFALSE
from .utils import AsyncMixin, AttrDict


class Queue(AsyncMixin):

    """Support message queue."""

    defaults = dict(
        host='localhost',
        port=None,
        login='guest',
        password='guest',
        virtualhost='/',
    )

    def __init__(self, core, loop=None, exchange='donald', queue='donald', **params):
        """Initialize the queue."""
        self.params = self.defaults
        self.params.update(params)

        self._core = core
        self._loop = loop or asyncio.get_event_loop()
        self._queue = queue
        self._transport = None
        self._protocol = None
        self._channel = None
        self._started = False
        self._connected = False

    def is_connected(self):
        """Check that the queue is connected."""
        return self._connected

    def start(self, listen=True):
        """Connect to message queue."""
        if self._core.params.always_eager:
            return False
        self._started = True
        return asyncio.ensure_future(self.connect(listen), loop=self.loop)

    @asyncio.coroutine
    def connect(self, listen=True):
        """Connect to queue."""
        logger.warning('Connect to queue.')
        try:
            self._transport, self._protocol = yield from aioamqp.connect(
                loop=self._loop, on_error=self.on_error, **self.params)
            self._channel = yield from self._protocol.channel()

            yield from self._channel.queue_declare(queue_name=self._queue, durable=True)
            yield from self._channel.basic_qos(
                prefetch_count=1, prefetch_size=0, connection_global=False)
            self._connected = True
            if listen:
                yield from self.listen()

        except (aioamqp.AmqpClosedConnection, OSError) as exc:
            self.on_error(exc)

    def on_error(self, exc):
        """Error callback."""
        self._connected = False
        if not self._started or self.is_closed():
            return False

        logger.error(exc)
        self.loop.call_later(1, asyncio.ensure_future, self.connect())

    def listen(self):
        """Run tasks from self.queue.

        :returns: A coroutine
        """
        if self._core.params.always_eager:
            return AIOFALSE
        return self._channel.basic_consume(self.callback, queue_name=self._queue)

    @asyncio.coroutine
    def stop(self):
        """Stop listeners."""
        if not self.is_connected() or self.is_closed():
            return False

        logger.warning('Disconnect from queue.')
        self._started = False
        yield from self._protocol.close()
        self._transport.close()
        self._connected = False

    def submit(self, func, *args, **kwargs):
        """Submit to the queue."""
        logger.info('Submit task to queue: %r', func)
        payload = pickle.dumps((func, args, kwargs))
        properties = dict(delivery_mode=2, message_id=str(uuid.uuid4()))

        if self._core.params.always_eager:
            return asyncio.ensure_future(
                self.callback(self._channel, payload, None, AttrDict(properties)), loop=self.loop)

        if not self._connected:
            return AIOFALSE

        if asyncio.iscoroutine(func):
            raise RuntimeError('Submit coroutines to queue as coroutine-functions with params.')

        coro = self._channel.basic_publish(
            payload=payload, exchange_name='', routing_key=self._queue, properties=properties
        )
        return asyncio.ensure_future(coro, loop=self.loop)

    @asyncio.coroutine
    def callback(self, channel, body, envelope, properties):
        """A Listener."""
        func, args, kwargs = pickle.loads(body)
        if isinstance(func, str):
            mod, _, func = func.rpartition('.')
            try:
                mod = import_module(mod)
                func = getattr(mod, func)
            except (ImportError, AttributeError) as exc:
                logger.error(exc)
                return False

        try:
            result = yield from self._core.submit(func, *args, **kwargs)
        except Exception as exc:  # noqa
            return self._core.handle_exc(sys.exc_info(), exc, func, *args, **kwargs)

        logger.info('Received result %r from message %r', result, properties.message_id)
        if channel:
            yield from channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
        return result
