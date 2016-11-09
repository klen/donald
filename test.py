import asyncio
import threading

from octopus.core import Octopus


counter = 0


@asyncio.coroutine
def coro():
    print('Start coro', id(threading.currentThread()))
    yield from asyncio.sleep(3)
    return id(threading.currentThread())


def ping():
    global counter
    counter += 1
    print('Pong %d' % counter)


def remote(name='world'):
    print('Hello %s from remote' % name)
    return 42


@asyncio.coroutine
def start():
    octo = Octopus(num_threads=3)
    yield from octo.start()

    octo.schedule(1, ping)

    #  yield from octo.queue.start(False)
    #  yield from octo.queue.listen()

    #  yield from octo.queue.submit(remote)
    #  yield from octo.queue.submit(remote)
    #  yield from octo.queue.submit(remote)
    #  yield from octo.queue.submit(remote)

    results = yield from asyncio.gather(*[
        octo.submit(coro()),
        octo.submit(coro()),
        octo.submit(coro()),
    ])
    yield from octo.stop()
    return results


loop = asyncio.get_event_loop()
results = loop.run_until_complete(start())

print(111, results)
