import asyncio
import threading

from donald.core import Donald


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


def exception():
    raise Exception('LA LA LA LA')


def job(func):
    donald.queue.submit(func)


donald = Donald()

@asyncio.coroutine
def start():
    donald = Donald(num_threads=3)
    yield from donald.start()

    donald.schedule(2, job, ping)
    donald.schedule(10, job, exception)
    donald.queue.start()

    #  donald.schedule(1, ping)

    #  #  yield from octo.queue.start(False)
    #  #  yield from octo.queue.listen()

    #  #  yield from octo.queue.submit(remote)
    #  #  yield from octo.queue.submit(remote)
    #  #  yield from octo.queue.submit(remote)
    #  #  yield from octo.queue.submit(remote)

    #  results = yield from asyncio.gather(*[
        #  donald.submit(coro()),
        #  donald.submit(coro()),
        #  donald.submit(coro()),
    #  ])
    #  yield from donald.stop()
    #  return results


loop = asyncio.get_event_loop()
results = loop.run_until_complete(start())
loop.run_forever()

#  print(111, results)
