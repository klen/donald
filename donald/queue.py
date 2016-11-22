"""AMPQ support."""
import aioamqp
import asyncio
import pickle
import uuid
from importlib import import_module


from . import logger
from .utils import AsyncMixin, AttrDict


class Queue(AsyncMixin):

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
        self._connected = False

    def is_connected(self):
        """Check that the queue is connected."""
        return self._connected

    @asyncio.coroutine
    def start(self, listen=True):
        """Connect to message queue."""
        logger.warn('Connect to queue.')
        try:
            self._transport, self._protocol = yield from aioamqp.connect(
                loop=self._loop, **self.params)
            self._channel = yield from self._protocol.channel()

            yield from self._channel.queue_declare(queue_name=self._queue, durable=True)
            yield from self._channel.basic_qos(
                prefetch_count=1, prefetch_size=0, connection_global=False)
            self._connected = True
            if listen:
                yield from self.listen()

        except aioamqp.AmqpClosedConnection as exc:
            logger.exception(exc)
            logger.error('Connection is closed.')

    def listen(self):
        """Run tasks from self.queue.

        :returns: A coroutine
        """
        return self._channel.basic_consume(self.callback, queue_name=self._queue)

    @asyncio.coroutine
    def stop(self):
        """Stop listeners."""
        if not self.is_connected() or self.is_closed():
            return False

        logger.warn('Disconnect from queue.')

        yield from self._protocol.close()
        self._transport.close()
        self._connected = False

    def submit(self, func, *args, **kwargs):
        """Submit to the queue."""
        logger.info('Submit task to queue.')

        payload = pickle.dumps((func, args, kwargs))
        properties = dict(delivery_mode=2, message_id=str(uuid.uuid4()))

        if self._core.params.always_eager:
            return self.callback(self._channel, payload, None, AttrDict(properties))

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

        result = yield from self._core.submit(func, *args, **kwargs)
        logger.info('Get result %r %r', result, properties.message_id)
        if channel:
            yield from channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
        return result
