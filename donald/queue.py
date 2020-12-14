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
        queue='donald',
        exchange_name='',
    )

    def __init__(self, master, **params):
        """Initialize the queue."""
        self.params = self.defaults
        self.params.update(params)
        self.master = master
        self.transport = None
        self.protocol = None
        self.channel = None

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

    async def start(self, listen=True, loop=None):
        """Connect and start listen the message queue."""
        if self.master.params.fake_mode:
            return AIOTRUE

        logger.warning('Start Donald Queue')

        self.init_loop(loop)
        self._started = True

        await self.connect()
        if listen:
            await self.listen()

    async def stop(self, *args, **kwargs):
        """Stop listeners."""
        if not self.is_connected() or self.is_closed() or not self._started:
            return False

        logger.warning('Stop Donald Queue')

        self._started = False
        await self.protocol.close()
        self.transport.close()
        self._connected = False

    async def connect(self):
        """Connect to queue."""
        if self._connected:
            return

        logger.warning('Connect to queue: %r', self.params)
        try:
            self.transport, self.protocol = await aioamqp.connect(
                loop=self._loop, on_error=self.on_error, **self.params)
            self.channel = await self.protocol.channel()

            await self.channel.queue_declare(queue_name=self.params['queue'], durable=True)
            await self.channel.basic_qos(
                prefetch_count=1, prefetch_size=0, connection_global=False)
            self._connected = True

        except (aioamqp.AmqpClosedConnection, OSError) as exc:
            self.on_error(exc)

    def on_error(self, exc):
        """Error callback."""
        self._connected = False
        if not self._started or self.is_closed():
            return False

        logger.error(exc)
        self.loop.call_later(1, aio.create_task, self.connect())

    def listen(self):
        """Run tasks from self.queue.

        :returns: A coroutine
        """
        if self.master.params.fake_mode:
            return AIOFALSE
        return self.channel.basic_consume(self.callback, queue_name=self.params['queue'])

    def submit(self, func, *args, **kwargs):
        """Submit to the queue."""
        if self.master.params.fake_mode:
            return self.master.submit(func, *args, **kwargs)

        logger.info('Submit to queue: %s', repr_func(func, args, kwargs))

        payload = pickle.dumps((func, args, kwargs))
        properties = dict(delivery_mode=2, message_id=str(uuid.uuid4()))

        if not self._connected:
            return AIOFALSE

        return aio.create_task(self.channel.basic_publish(
            payload=payload, exchange_name=self.params['exchange_name'],
            routing_key=self.params['queue'], properties=properties
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
