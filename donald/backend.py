import asyncio
import random
from threading import local
from typing import AsyncIterator, Dict, Type
from urllib.parse import urlparse
from uuid import uuid4

from . import logger
from .types import TBackendType, TRunArgs
from .utils import dumps, loads

# from dill import dumps, loads


current_backend = local()
current_backend.value = None


class BaseBackend:

    type: TBackendType
    defaults: Dict = {}

    def __init__(self, params: Dict):
        self.is_connected = False
        self.params = dict(self.defaults, **params)

    async def connect(self):
        await self._connect()
        current_backend.value = self
        self.is_connected = True
        logger.info("Backend connected: %s", self.type)

    async def disconnect(self):
        self.is_connected = False
        await self._disconnect()
        current_backend.value = None
        logger.info("Backend disconnected: %s", self.type)

    def submit(self, data: TRunArgs):
        if not self.is_connected:
            raise RuntimeError("Backend is not connected")
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

    rx = None

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

    async def _connect(self):
        self.rx = asyncio.Queue()

    async def _submit(self, data):
        self.rx.put_nowait(data)
        return True


class RedisBackend(BaseBackend):
    type: TBackendType = "redis"
    defaults = {
        "url": "redis://localhost:6379/0",
        "channel": "donald",
    }
    redis = None

    async def _connect(self):
        from redis.asyncio import from_url

        self.redis = from_url(self.params["url"])

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

    transport = protocol = channel = None

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
            self.transport, self.protocol = await connect(
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

        self.channel = await self.protocol.channel()
        await self.channel.queue_declare(queue_name=self.params["queue"], durable=True)
        await self.channel.basic_qos(
            prefetch_count=1, prefetch_size=0, connection_global=False
        )

    async def _disconnect(self):
        await self.protocol.close(no_wait=True)
        self.transport.close()

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

        await self.channel.basic_consume(consumer, self.params["queue"])

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
