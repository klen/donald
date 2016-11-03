import pytest
import time
import asyncio
import threading
from octopus import Octopus


async def coro(num=0):
    print('Start coro', id(threading.currentThread()))
    await asyncio.sleep(2)
    print('Stop coro', id(threading.currentThread()))
    return 'coro%d' % num


def func(num=0):
    print('Start func', id(threading.currentThread()))
    time.sleep(2)
    print('Stop func', id(threading.currentThread()))
    return 'func%d' % num


@asyncio.coroutine
def ping():
    print('Pong')


@pytest.mark.asyncio
async def test_octopus():
    octo = Octopus(num_threads=3, loglevel='debug')
    await octo.start()
    octo.schedule(.2, ping)
    results = await asyncio.gather(*[
        octo.submit(coro()),
        octo.submit(coro, 1),
        octo.submit(func, 2),
    ])
    octo.params.always_eager = True
    await octo.stop()
    assert results == ['coro0', 'coro1', 'func2']

    result = await octo.submit(coro, 3)
    assert result == 'coro3'
