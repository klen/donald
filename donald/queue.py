"""AMPQ support."""
import asyncio
import pickle
import typing as t
import uuid
from importlib import import_module

import aioamqp

from . import logger
from .utils import AsyncMixin


class Queue(AsyncMixin):

    """Support message queue."""

    defaults = dict(
        host="localhost",
        port=None,
        login="guest",
        password="guest",
        virtualhost="/",
        queue="donald",
        exchange_name="",
    )

    _started: bool = False
    _connected: bool = False
    _loop: t.Optional[asyncio.AbstractEventLoop] = None

    transport = None
    protocol: t.Optional[aioamqp.protocol.AmqpProtocol] = None
    channel: t.Optional[aioamqp.channel.Channel] = None

    def __init__(self, master: "Donald", **params):
        """Initialize the queue."""
        self.params = dict(self.defaults, **params)
        self.master = master

    def init_loop(self, loop: t.Optional[asyncio.AbstractEventLoop]):
        """Bind to given loop."""
        if not self._started:
            self._loop = loop or asyncio.get_event_loop()

    def is_connected(self) -> bool:
        """Check that the queue is connected."""
        return self._connected

    async def start(
        self, listen: bool = True, loop: asyncio.AbstractEventLoop = None
    ) -> bool:
        """Connect and start listen the message queue."""
        if self.master.params["fake_mode"]:
            return True

        logger.warning("Start Donald Queue")

        self.init_loop(loop)
        self._started = True

        await self.connect()
        if listen:
            channel = t.cast(aioamqp.channel.Channel, self.channel)
            await channel.basic_consume(self.listen, queue_name=self.params["queue"])

        return self._started

    async def stop(self, *args, **kwargs):
        """Stop listeners."""
        if not self.is_connected() or self.is_closed() or not self._started:
            return False

        self._started = False

        await self.protocol.close(no_wait=True)
        self.transport.close()
        self._connected = False

        logger.warning("Donald Queue is stopped")

    async def __aenter__(self) -> "Queue":
        """Support usage as a context manager."""
        await self.start()
        return self

    async def __aexit__(self, *args):
        """Support usage as a context manager."""
        await self.stop()

    async def connect(self) -> bool:
        """Connect to queue."""
        if not self._connected:
            logger.warning("Connect to queue: %r", self.params)
            try:
                self.transport, self.protocol = await asyncio.wait_for(
                    aioamqp.connect(
                        loop=self._loop, on_error=self.on_error, **self.params
                    ),
                    timeout=10,
                )
                self.channel = await self.protocol.channel()
                await self.channel.queue_declare(
                    queue_name=self.params["queue"], durable=True
                )
                await self.channel.basic_qos(
                    prefetch_count=1, prefetch_size=0, connection_global=False
                )
                self._connected = True

            except Exception as exc:
                self.on_error(exc)
                raise

        return self._connected

    def on_error(self, exc: Exception):
        """Reconnect on errors."""
        self._connected = False
        if self._started and not self.is_closed():
            logger.error(exc)
            self.loop.call_later(1, asyncio.create_task, self.connect())

    def submit(self, func: t.Callable, *args, **kwargs) -> asyncio.Future:
        """Submit to the queue."""
        if self.master.params["fake_mode"]:
            return self.master.submit(func, *args, **kwargs)

        logger.info("Submit to queue: '%s'", func.__qualname__)

        payload = pickle.dumps((func, args, kwargs))
        properties = dict(delivery_mode=2, message_id=str(uuid.uuid4()))

        if not self._connected:
            logger.warning("Donald Queue is not connected")
            fut: asyncio.Future = asyncio.Future()
            fut.set_result(False)
            return fut

        channel = t.cast(aioamqp.channel.Channel, self.channel)
        return asyncio.create_task(
            channel.basic_publish(
                payload=payload,
                exchange_name=self.params["exchange_name"],
                routing_key=self.params["queue"],
                properties=properties,
            )
        )

    async def listen(
        self,
        channel: aioamqp.channel.Channel,
        body: bytes,
        envelope: aioamqp.envelope.Envelope,
        properties: aioamqp.properties.Properties,
    ):
        """Run tasks from self.queue."""
        func, args, kwargs = pickle.loads(body)
        if isinstance(func, str):
            mod_name, _, func = func.rpartition(".")
            try:
                mod = import_module(mod_name)
                func = getattr(mod, func)
            except (ImportError, AttributeError) as exc:
                logger.error(exc)
                return False

        self.master.submit_nowait(func, *args, **kwargs)

        if channel:
            await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


if t.TYPE_CHECKING:
    from .core import Donald  # noqa
