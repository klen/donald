from __future__ import annotations

from abc import abstractmethod
from asyncio import Future, Queue, get_running_loop, sleep
from asyncio import timeout as async_timeout
from pickle import dumps, loads
from typing import TYPE_CHECKING, Any, AsyncIterator, ClassVar, Coroutine, Mapping, cast
from uuid import uuid4

from aio_pika import DeliveryMode, Exchange, Message, connect_robust
from aio_pika.abc import AbstractExchange

from .utils import BackendNotReadyError, logger

if TYPE_CHECKING:
    from aio_pika import Channel
    from aio_pika.abc import AbstractChannel
    from redis.asyncio import Redis

    from donald.types import TTaskParams

    from .types import TBackendType, TRunArgs


class BaseBackend:
    backend_type: TBackendType
    defaults: ClassVar[Mapping] = {}

    def __init__(self, params: Mapping):
        self.is_connected = False
        self.params = dict(self.defaults, **params)
        self.__backend__: Any = None

    async def connect(self):
        logger.info("Connecting to Tasks Backend: %s", self.backend_type)
        await self._connect()
        self.is_connected = True
        logger.info("Connected to Tasks Backend: %s", self.backend_type)

    async def disconnect(self):
        logger.info("Disconnecting from Tasks Backend: %s", self.backend_type)
        self.is_connected = False
        await self._disconnect()
        logger.info("Disconnected from Tasks Backend: %s", self.backend_type)

    def submit(self, data: TRunArgs):
        if not self.is_connected:
            raise BackendNotReadyError
        _data = dumps(data)
        return self._submit(_data)

    async def submit_and_wait(self, data: TRunArgs, timeout=10):
        if not self.is_connected:
            raise BackendNotReadyError

        return await self._submit_and_wait(data, timeout=timeout)

    @abstractmethod
    async def _connect(self): ...

    @abstractmethod
    async def _disconnect(self): ...

    @abstractmethod
    async def _submit(self, data) -> Any: ...

    @abstractmethod
    async def _submit_and_wait(self, data, timeout=10): ...

    @abstractmethod
    async def callback(self, result, reply_to: str, correlation_id: str): ...

    @abstractmethod
    def subscribe(self) -> Coroutine[Any, Any, AsyncIterator[TRunArgs]]: ...


class MemoryBackend(BaseBackend):
    backend_type: TBackendType = "memory"

    def __init__(self, params: Mapping):
        super().__init__(params)
        self.__pending__: dict[str, Future] = {}

    @property
    def rx(self) -> Queue:
        if self.__backend__ is None:
            raise BackendNotReadyError

        return self.__backend__

    async def _connect(self):
        if self.__backend__ is None:
            self.__backend__ = Queue()

    async def _disconnect(self):
        self.is_connected = False
        self.__backend__ = None

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
                await sleep(0)

        return iter_tasks()

    async def callback(self, result, reply_to: str, correlation_id: str):
        future = self.__pending__.pop(correlation_id, None)
        if future is not None and not future.done():
            future.set_result(result)

    async def _submit_and_wait(self, data: TRunArgs, timeout=10):
        correlation_id = str(uuid4())
        task_name, args, kwargs, params = data
        _params: TTaskParams = {**params, "correlation_id": correlation_id, "reply_to": "results"}
        data = (task_name, args, kwargs, _params)
        await self._submit(dumps(data))

        future = get_running_loop().create_future()
        self.__pending__[correlation_id] = future
        try:
            async with async_timeout(timeout):
                return await future
        except TimeoutError as exc:
            raise TimeoutError("Task result timeout") from exc
        finally:
            self.__pending__.pop(correlation_id, None)


class RedisBackend(BaseBackend):
    backend_type: TBackendType = "redis"
    defaults: ClassVar = {
        "url": "redis://localhost:6379/0",
        "channel": "donald",
        "consumer": "consumer-1",
    }

    def __init__(self, params: Mapping):
        self.is_connected = False
        self.params = dict(self.defaults, **params)
        self.__backend__: Any = None

    @property
    def redis(self) -> "Redis":
        if self.__backend__ is None:
            raise BackendNotReadyError
        return self.__backend__

    async def _connect(self):
        from redis.asyncio import from_url

        self.__backend__ = from_url(self.params["url"])

        stream = f"{self.params['channel']}:stream"
        group = f"{self.params['channel']}:group"

        try:
            await self.redis.xgroup_create(stream, group, id="$", mkstream=True)
            logger.info("Created Redis stream group '%s'", group)
        except Exception:  # noqa: BLE001
            logger.info("Redis stream group '%s' already exists", group)

        self.is_connected = True

    async def _disconnect(self):
        await self.redis.aclose()  # type: ignore[]
        self.is_connected = False

    async def _submit(self, data):
        stream = f"{self.params['channel']}:stream"
        return await self.redis.xadd(stream, {"data": data})

    async def subscribe(self):
        stream = f"{self.params['channel']}:stream"
        group = f"{self.params['channel']}:group"
        consumer = self.params["consumer"]

        async def iter_tasks():
            while self.is_connected:
                msgs = await self.redis.xreadgroup(
                    group, consumer, streams={stream: ">"}, count=1, block=1000
                )
                if not msgs:
                    continue

                for _, entries in msgs:
                    entries = cast(Any, entries)  # noqa: TC006
                    for msg_id, msg_data in entries:
                        data = msg_data.get(b"data")
                        if data:
                            yield loads(data)
                            await self.redis.xack(stream, group, msg_id)

        return iter_tasks()

    async def callback(self, result, reply_to: str, correlation_id: str):
        payload = dumps(
            {
                "result": result,
                "correlation_id": correlation_id,
            }
        )
        # For Streams-based RPC, we can implement a separate stream for results
        stream = f"{reply_to}:stream"
        await self.redis.xadd(stream, {"data": payload})

    async def _submit_and_wait(self, data: TRunArgs, timeout=10):
        correlation_id = str(uuid4())
        task_name, args, kwargs, params = data
        params["correlation_id"] = correlation_id
        reply_to = f"{self.params['channel']}:rpc:{correlation_id}"
        params["reply_to"] = reply_to
        data = (task_name, args, kwargs, params)

        # Submit task
        await self._submit(dumps(data))

        # Wait for result on reply stream
        stream = f"{reply_to}:stream"
        try:
            async with async_timeout(timeout):
                while True:
                    msgs = await self.redis.xread({stream: "0"}, block=1000, count=1)
                    if not msgs:
                        continue
                    for _, entries in msgs:
                        entries = cast(Any, entries)  # noqa: TC006
                        for msg_id, msg_data in entries:
                            res = loads(msg_data[b"data"])
                            if res.get("correlation_id") == correlation_id:
                                await self.redis.xdel(stream, msg_id)
                                return res.get("result")
        except TimeoutError as exc:
            raise TimeoutError("Task result timeout") from exc


class AMQPBackend(BaseBackend):
    backend_type: TBackendType = "amqp"
    defaults: ClassVar = {
        "url": "amqp://guest:guest@localhost:5672/",
        "queue": "donald",
        "exchange": "",
    }

    def __init__(self, params: Mapping):
        super().__init__(params)
        self.__channel__: Channel | AbstractChannel | None = None
        self.__exchange__: Exchange | AbstractExchange | None = None

    async def _connect(self):
        self.__backend__ = await connect_robust(self.params["url"])
        self.__channel__ = channel = await self.__backend__.channel()

        await channel.set_qos(prefetch_count=1)
        self.__queue__ = await channel.declare_queue(self.params["queue"], durable=True)

        exchange_name = self.params["exchange"]
        if exchange_name:
            self.exchange = await channel.get_exchange(exchange_name)
        else:
            self.exchange = channel.default_exchange

    async def _disconnect(self):
        await self.__backend__.close()

    async def _submit(self, data, **params):
        return await self.exchange.publish(
            Message(
                body=data,
                message_id=str(uuid4()),
                delivery_mode=DeliveryMode.PERSISTENT,
                **params,
            ),
            routing_key=self.params["queue"],
        )

    async def subscribe(self):
        queue = self.__queue__

        async def iter_tasks() -> AsyncIterator[TRunArgs]:
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        data: TRunArgs = loads(message.body)
                        if message.reply_to and message.correlation_id:
                            task_name, args, kwargs, params = data
                            _params: TTaskParams = {
                                **params,
                                "reply_to": message.reply_to,
                                "correlation_id": message.correlation_id,
                            }
                            data = (task_name, args, kwargs, _params)

                        yield data

        return iter_tasks()

    async def callback(self, result, reply_to: str, correlation_id: str):
        await self.exchange.publish(
            Message(
                body=dumps(result),
                correlation_id=correlation_id,
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key=reply_to,
        )

    async def _submit_and_wait(self, data: TRunArgs, timeout=10):
        channel = self.__channel__
        assert channel is not None, "Channel is not set"
        callback_queue = await channel.declare_queue(exclusive=True)

        correlation_id = str(uuid4())

        await self._submit(
            dumps(data),
            reply_to=callback_queue.name,
            correlation_id=correlation_id,
        )

        async with callback_queue.iterator() as queue_iter:
            try:
                async with async_timeout(timeout):
                    async for message in queue_iter:
                        async with message.process():
                            if message.correlation_id == correlation_id:
                                return loads(message.body)
            except TimeoutError as exc:
                raise TimeoutError("Task result timeout") from exc

    @property
    def exchange(self) -> Exchange | AbstractExchange:
        if self.__exchange__ is None:
            raise BackendNotReadyError
        return self.__exchange__

    @exchange.setter
    def exchange(self, value: Exchange | AbstractExchange):
        if not isinstance(value, AbstractExchange):
            raise TypeError(f"Expected AbstractExchange, got {type(value).__name__}")
        self.__exchange__ = value


BACKENDS: dict[TBackendType, type[BaseBackend]] = {
    "memory": MemoryBackend,
    "redis": RedisBackend,
    "amqp": AMQPBackend,
}

# ruff: noqa: S301, ARG002, PLC0415
