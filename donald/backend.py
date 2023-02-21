import asyncio
import random
from pickle import dumps, loads
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, Mapping, Type
from urllib.parse import urlparse
from uuid import uuid4

from . import logger
from .types import TBackendType, TRunArgs

if TYPE_CHECKING:
    from aioamqp import Channel
    from redis.asyncio import Redis


class BaseBackend:

    type: TBackendType
    defaults: Mapping = {}

    def __init__(self, params: Mapping):
        self.is_connected = False
        self.params = dict(self.defaults, **params)
        self.__backend__: Any = None

    async def connect(self):
        logger.info("Connecting to Tasks Backend: %s", self.type)
        await self._connect()
        self.is_connected = True
        logger.info("Connected to Tasks Backend: %s", self.type)

    async def disconnect(self):
        logger.info("Disconnecting from Tasks Backend: %s", self.type)
        self.is_connected = False
        await self._disconnect()
        logger.info("Disconnected from Tasks Backend: %s", self.type)

    def submit(self, data: TRunArgs):
        if not self.is_connected:
            raise RuntimeError("Tasks Backend is not connected")
        _data = dumps(data)
        return self._submit(_data)

    async def subscribe(self) -> AsyncIterator[TRunArgs]:
        raise NotImplementedError

    async def _connect(self):
        raise NotImplementedError

    async def _disconnect(self):
        pass

    async def _submit(self, data):
        raise NotImplementedError


class MemoryBackend(BaseBackend):
    type: TBackendType = "memory"

    @property
    def rx(self) -> asyncio.Queue:
        if self.__backend__ is None:
            raise RuntimeError("Tasks Backend is not connected")

        return self.__backend__

    async def _connect(self):
        self.__backend__ = asyncio.Queue()

    async def _submit(self, data):
        self.rx.put_nowait(data)
        return True

    async def subscribe(self):
        await self.connect()

        async def iter_tasks():
            while self.is_connected:
                msg = await self.rx.get()
                if msg is None:
                    continue

                yield loads(msg)
                await asyncio.sleep(0)

        return iter_tasks()


class RedisBackend(BaseBackend):
    type: TBackendType = "redis"
    defaults = {
        "url": "redis://localhost:6379/0",
        "channel": "donald",
    }

    @property
    def redis(self) -> "Redis":
        if self.__backend__ is None:
            raise RuntimeError("Tasks Backend is not connected")

        return self.__backend__

    async def _connect(self):
        from redis.asyncio import from_url

        self.__backend__ = from_url(self.params["url"])

    async def _disconnect(self):
        await self.redis.close()

    async def _submit(self, data):
        return await self.redis.publish(f"{self.params['channel']}:1", data)

    async def subscribe(self):
        pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        await pubsub.subscribe(f"{self.params['channel']}:1")

        async def iter_tasks():
            while self.is_connected:
                msg = await pubsub.get_message()
                if msg is None:
                    await asyncio.sleep(1e-2)
                    continue

                yield loads(msg["data"])

        return iter_tasks()


class AMQPBackend(BaseBackend):
    type: TBackendType = "amqp"
    defaults = {
        "url": "amqp://guest:guest@localhost:5672/",
        "queue": "donald",
        "exchange": "",
    }

    def __init__(self, params: Mapping):
        super().__init__(params)
        self.__protocol__ = None
        self.__channel__ = None

    @property
    def channel(self) -> "Channel":
        if self.__backend__ is None:
            raise RuntimeError("Tasks Backend is not connected")

        return self.__channel__

    async def _connect(self):
        from aioamqp import connect

        url = urlparse(self.params["url"])
        host, port, vhost, user, password = (
            url.hostname,
            url.port,
            url.path,
            url.username,
            url.password,
        )

        try:
            self.__backend__, self.__protocol__ = await connect(
                host=host,
                port=port,
                virtualhost=vhost,
                login=user,
                password=password,
                on_error=self.on_error,
                **self.params,
            )
        except OSError as exc:
            logger.exception("Failed to connect to AMQP")
            return asyncio.create_task(self.on_error(exc))

        self.__channel__ = await self.__protocol__.channel()
        await self.__channel__.queue_declare(
            queue_name=self.params["queue"], durable=True
        )
        await self.__channel__.basic_qos(
            prefetch_count=1, prefetch_size=0, connection_global=False
        )

    async def _disconnect(self):
        await self.__protocol__.close(no_wait=True)
        self.__backend__.close()

    reconnecting = None

    async def on_error(self, exc):
        if not self.is_connected:
            return

        self.is_connected = False
        if self.reconnecting is None:
            self.reconnecting = asyncio.Condition()

        if self.reconnecting.locked():
            return

        async with self.reconnecting:
            logger.exception("AMQP Connection Error, reconnecting in 5 seconds")
            await asyncio.sleep(5 + random.random())
            try:
                await self.connect()
            except Exception:
                pass

    async def _submit(self, data):
        await self.channel.basic_publish(
            payload=data,
            exchange_name=self.params["exchange"],
            routing_key=self.params["queue"],
            properties=dict(delivery_mode=2, message_id=f"{uuid4()}"),
        )
        return True

    async def subscribe(self):
        rx = asyncio.Queue()

        async def consumer(channel, body, envelope, properties):
            rx.put_nowait(body)
            await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

        await self.__channel__.basic_consume(consumer, self.params["queue"])

        async def iter_tasks():
            while self.is_connected:
                msg = await rx.get()
                if msg is None:
                    continue

                yield loads(msg)

        return iter_tasks()


BACKENDS: Dict[TBackendType, Type[BaseBackend]] = {
    "memory": MemoryBackend,
    "redis": RedisBackend,
    "amqp": AMQPBackend,
}
