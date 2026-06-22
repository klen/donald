from unittest.mock import MagicMock

from aio_pika.abc import AbstractExchange

from donald.backend import AMQPBackend


def test_amqp_backend_accepts_default_exchange():
    """The exchange setter must accept AbstractExchange (e.g., channel.default_exchange)."""
    backend = AMQPBackend({})
    default_exchange = MagicMock(spec=AbstractExchange)

    backend.exchange = default_exchange

    assert backend.exchange is default_exchange
