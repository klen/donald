import asyncio
from random import random

from donald import Donald

manager = Donald(
    backend="amqp",
    log_level="DEBUG",
    worker={"max_tasks": 2},
)


@manager.schedule(5)
@manager.task
async def task_5():
    if random() > 0.5:
        task_r.submit()
    await asyncio.sleep(random() * 5)
    print("Result: 5")


@manager.schedule(10)
@manager.task
async def task_10():
    await asyncio.sleep(random() * 10)
    print("Result: 10")


@manager.task(bind=True)  # type: ignore
async def task_r(self, retry=False):
    await asyncio.sleep(random())
    if random() > 0.5:
        self.retry()

    print("Result: R, retry:", self.retries)
