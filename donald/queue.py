import aioamqp
import asyncio
import pickle
import uuid


from . import logger
from .utils import AsyncMixin


class Queue(AsyncMixin):

    defaults = dict(
        host='localhost',
        port=None,
        login='guest',
        password='guest',
        virtualhost='/',
    )

    def __init__(self, coro, loop=None, exchange='donald', queue='donald', **params):
        self.params = self.defaults
        self.params.update(params)

        self._coro = coro
        self._loop = loop or asyncio.get_event_loop()
        self._queue = queue
        self._transport = None
        self._protocol = None
        self._channel = None
        self._connected = False

    def is_connected(self):
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
        if not self.is_connected() or self.is_closed():
            return False

        logger.warn('Disconnect from queue.')

        yield from self._protocol.close()
        self._transport.close()
        self._connected = False

    @asyncio.coroutine
    def submit(self, func, *args, **kwargs):
        logger.info('Submit task to queue.')
        yield from self._channel.basic_publish(
            payload=pickle.dumps((func, args, kwargs)),
            exchange_name='', routing_key=self._queue,
            properties=dict(delivery_mode=2, message_id=str(uuid.uuid4()))
        )

    @asyncio.coroutine
    def callback(self, channel, body, envelope, properties):
        func, args, kwargs = pickle.loads(body)
        result = yield from self._coro.submit(func, *args, **kwargs)
        logger.info('Get result %r %r', result, properties.message_id)
        yield from channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
