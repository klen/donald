"""AMPQ support."""
import asyncio as aio
import pickle
import uuid
from importlib import import_module

import aioamqp

from . import logger, AIOFALSE, AIOTRUE
from .utils import AsyncMixin, repr_func


class Queue(AsyncMixin):

    """Support message queue."""

    defaults = dict(
        host='localhost',
        port=None,
        login='guest',
        password='guest',
        virtualhost='/',
    )

    def __init__(self, master, exchange='donald', queue='donald', **params):
        """Initialize the queue."""
        self.params = self.defaults
        self.params.update(params)
        self.master = master

        self._queue = queue
        self._transport = None
        self._protocol = None
        self._channel = None
        self._started = False
        self._connected = False
        self._loop = None

    def init_loop(self, loop):
        """Bind to given loop."""
        if not self._started:
            self._loop = loop or aio.get_event_loop()

    def is_connected(self):
        """Check that the queue is connected."""
        return self._connected

    def start(self, listen=True, loop=None):
        """Connect to message queue."""
        if self.master.params.fake_mode:
            return AIOTRUE
        self.init_loop(loop)
        self._started = True
        return aio.create_task(self.connect(listen))

    async def connect(self, listen=True):
        """Connect to queue."""
        logger.warning('Connect to queue: %r', self.params)
        try:
            self._transport, self._protocol = await aioamqp.connect(
                loop=self._loop, on_error=self.on_error, **self.params)
            self._channel = await self._protocol.channel()

            await self._channel.queue_declare(queue_name=self._queue, durable=True)
            await self._channel.basic_qos(
                prefetch_count=1, prefetch_size=0, connection_global=False)
            self._connected = True
            if listen:
                await self.listen()

        except (aioamqp.AmqpClosedConnection, OSError) as exc:
            self.on_error(exc)

    def on_error(self, exc):
        """Error callback."""
        self._connected = False
        if not self._started or self.is_closed():
            return False

        logger.error(exc)
        self.loop.call_later(1, aio.ensure_future, self.connect())

    def listen(self):
        """Run tasks from self.queue.

        :returns: A coroutine
        """
        if self.master.params.fake_mode:
            return AIOFALSE
        return self._channel.basic_consume(self.callback, queue_name=self._queue)

    async def stop(self):
        """Stop listeners."""
        if not self.is_connected() or self.is_closed() or not self._started:
            return False

        logger.warning('Disconnect from queue.')
        self._started = False
        await self._protocol.close()
        self._transport.close()
        self._connected = False

    def submit(self, func, *args, **kwargs):
        """Submit to the queue."""
        if self.master.params.fake_mode:
            return self.master.submit(func, *args, **kwargs)

        logger.info('Submit to queue: %s', repr_func(func, args, kwargs))

        payload = pickle.dumps((func, args, kwargs))
        properties = dict(delivery_mode=2, message_id=str(uuid.uuid4()))

        if not self._connected:
            return AIOFALSE

        return aio.create_task(self._channel.basic_publish(
            payload=payload, exchange_name='', routing_key=self._queue, properties=properties
        ))

    async def callback(self, channel, body, envelope, properties):
        """Do a callback."""
        func, args, kwargs = pickle.loads(body)
        if isinstance(func, str):
            mod, _, func = func.rpartition('.')
            try:
                mod = import_module(mod)
                func = getattr(mod, func)
            except (ImportError, AttributeError) as exc:
                logger.error(exc)
                return False

        fut = self.master.submit(func, *args, **kwargs)
        if channel:
            await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

        return fut
