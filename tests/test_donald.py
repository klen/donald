import asyncio
import threading
import time

import pytest

from donald import Donald


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
    print('Pong', time.time())


def exception():
    raise Exception('Test exception.')


@pytest.mark.asyncio
async def test_donald():
    donald = Donald(num_threads=3, loglevel='debug')
    await donald.start()
    donald.schedule(.2, ping)
    results = await asyncio.gather(*[
        donald.submit(coro()),
        donald.submit(coro, 1),
        donald.submit(func, 2),
    ])
    await donald.stop()
    assert results == ['coro0', 'coro1', 'func2']

    donald.params.always_eager = True
    result = await donald.submit(coro, 3)
    assert result == 'coro3'

    result = await donald.queue.submit(coro, 3)
    assert result == 'coro3'
