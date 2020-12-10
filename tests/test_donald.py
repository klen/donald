import asyncio as aio
from . import tasks

import pytest


@pytest.fixture
async def donald():
    from donald import Donald

    donald = Donald(num_workers=2, loglevel='DEBUG')

    await donald.start()
    yield donald
    await donald.stop()


@pytest.mark.asyncio
async def test_base(donald):
    assert not donald.waiting

    donald.submit(tasks.blocking, 1, key='22')
    donald.submit(tasks.async_blocking, 2)
    assert donald.waiting

    results = await aio.gather(
        donald.submit(tasks.blocking, 3),
        donald.submit(tasks.async_blocking, 4),
    )
    assert results == [3, 4]
    assert not donald.waiting

    donald.submit(tasks.async_wait, 100)
    assert donald.waiting


@pytest.mark.asyncio
async def test_exception(donald):

    with pytest.raises(Exception):
        await donald.submit(tasks.exception)


@pytest.mark.asyncio
async def test_schedule(donald):
    task = donald.schedule(.1, tasks.async_wait, 10)
    assert task
    assert not donald.waiting
    await aio.sleep(0.3)
    assert len(donald.waiting) == 2


@pytest.mark.asyncio
async def test_fake_mode(donald):
    donald.params.fake_mode = True
    task = donald.submit(tasks.blocking, 42)
    assert isinstance(task, aio.Task)
    res = await task
    assert res == 42

    res = await donald.queue.submit(tasks.blocking, 42)
    assert res == 42


@pytest.mark.asyncio
async def test_worker_cycle():
    from donald import Donald

    donald = Donald(num_workers=2, loglevel='DEBUG')
    donald.on_start(tasks.app_start)
    donald.on_stop(tasks.app_stop)

    await donald.start()

    res = await donald.submit(tasks.get_app_status)
    assert res == 'STARTED'

    await donald.stop()
