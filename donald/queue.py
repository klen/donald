"""AMPQ support."""
import asyncio
import pickle
import uuid
from importlib import import_module

import aioamqp

from . import logger, AIOFALSE
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
            self._loop = loop or asyncio.get_event_loop()

    def is_connected(self):
        """Check that the queue is connected."""
        return self._connected

    async def start(self, listen=True, loop=None):
        """Connect and start listen the message queue."""
        if self.master.params.fake_mode:
            return True

        logger.warning('Start Donald Queue')

        self.init_loop(loop)
        self._started = True

        await self.connect()
        if listen:
            await self.channel.basic_consume(self.listen, queue_name=self.params['queue'])

    async def stop(self, *args, **kwargs):
        """Stop listeners."""
        if not self.is_connected() or self.is_closed() or not self._started:
            return False

        self._started = False

        await self.protocol.close(no_wait=True)
        self.transport.close()
        self._connected = False

        logger.warning('Donald Queue is stopped')

    async def __aenter__(self):
        """Support usage as a context manager."""
        await self.start()
        return self

    async def __aexit__(self, *args):
        """Support usage as a context manager."""
        await self.stop()

    async def connect(self):
        """Connect to queue."""
        if self._connected:
            return

        logger.warning('Connect to queue: %r', self.params)
        try:
            self.transport, self.protocol = await asyncio.wait_for(aioamqp.connect(
                loop=self._loop, on_error=self.on_error, **self.params), timeout=10
            )
            self.channel = await self.protocol.channel()
            await self.channel.queue_declare(queue_name=self.params['queue'], durable=True)
            await self.channel.basic_qos(
                prefetch_count=1, prefetch_size=0, connection_global=False)
            self._connected = True
        except Exception as exc:
            self.on_error(exc)
            raise

    def on_error(self, exc):
        """Error callback."""
        self._connected = False
        if not self._started or self.is_closed():
            return False

        logger.error(exc)
        self.loop.call_later(1, asyncio.create_task, self.connect())

    def submit(self, func, *args, **kwargs):
        """Submit to the queue."""
        if self.master.params.fake_mode:
            return self.master.submit(func, *args, **kwargs)

        logger.info('Submit to queue: %s', repr_func(func, args, kwargs))

        payload = pickle.dumps((func, args, kwargs))
        properties = dict(delivery_mode=2, message_id=str(uuid.uuid4()))

        if not self._connected:
            return AIOFALSE

        return asyncio.create_task(self.channel.basic_publish(
            payload=payload, exchange_name=self.params['exchange_name'],
            routing_key=self.params['queue'], properties=properties
        ))

    async def listen(self, channel, body, envelope, properties):
        """Run tasks from self.queue."""
        func, args, kwargs = pickle.loads(body)
        if isinstance(func, str):
            mod, _, func = func.rpartition('.')
            try:
                mod = import_module(mod)
                func = getattr(mod, func)
            except (ImportError, AttributeError) as exc:
                logger.error(exc)
                return False

        self.master.submit(func, *args, **kwargs)
        if channel:
            await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
