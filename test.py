import asyncio as aio
from donald import Donald, crontab, logger


counter = 0


async def coro():
    logger.info('Start coro')
    await aio.sleep(3)


def ping():
    global counter
    counter += 1
    logger.info('Pong %d' % counter)


def remote(name='world'):
    print('Hello %s from remote' % name)
    return 42


def exception():
    raise Exception('LA LA LA LA')


def job(func):
    donald.queue.submit(func)


async def wait(num):
    logger.info('Wait for %s', num)
    await aio.sleep(num)
    return 'done'




async def start():
    donald = Donald(num_workers=2)
    await donald.start()
    #  await aio.sleep(1)
    donald.submit(wait, 10)
    donald.submit(wait, 20)
    donald.schedule(.5, ping)
    #  donald.schedule(crontab('* * * * *'), ping)
    #  donald.schedule(10, job, exception)
    logger.info('sleep a little')
    await aio.sleep(4)
    await donald.stop()
    #  #  donald.queue.start()

    #  #  donald.schedule(1, ping)

    #  #  #  yield from octo.queue.start(False)
    #  #  #  yield from octo.queue.listen()

    #  #  #  yield from octo.queue.submit(remote)
    #  #  #  yield from octo.queue.submit(remote)
    #  #  #  yield from octo.queue.submit(remote)
    #  #  #  yield from octo.queue.submit(remote)

    #  #  results = yield from asyncio.gather(*[
        #  #  donald.submit(coro()),
        #  #  donald.submit(coro()),
        #  #  donald.submit(coro()),
    #  #  ])
    #  #  yield from donald.stop()
    #  #  return results
    pass


if __name__ == '__main__':
    loop = aio.get_event_loop()
    results = loop.run_until_complete(start())
#  loop.run_forever()

#  print(111, results)

# pylama:ignore=D
