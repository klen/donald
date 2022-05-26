import asyncio

import pytest

from . import tasks


@pytest.fixture
async def donald():
    from donald import Donald

    async with Donald(num_workers=2, loglevel="DEBUG") as donald:
        yield donald


async def test_base(donald):
    assert not donald.waiting

    donald.submit(tasks.blocking, 1, key="22")
    donald.submit(tasks.async_blocking, 2)
    assert donald.waiting

    results = await asyncio.gather(
        donald.submit(tasks.blocking, 3),
        donald.submit(tasks.async_blocking, 4),
    )
    assert results == [3, 4]
    assert not donald.waiting

    donald.submit(tasks.async_wait, 100)
    assert donald.waiting

    results = await donald.submit(tasks.run_separate_task)
    assert results == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


async def test_exception(donald):

    with pytest.raises(Exception):
        await donald.submit(tasks.exception)


async def test_schedule(tmp_path):
    from donald import Donald

    filepath = tmp_path / "task"
    donald = Donald(num_workers=2, loglevel="DEBUG")
    assert not donald.schedules

    donald.schedule(0.1, filepath, "done")(tasks.write_file)
    assert donald.schedules

    await donald.start()
    assert not donald.waiting

    await asyncio.sleep(0.3)
    assert filepath.read_text() == "done"

    await donald.stop()


async def test_fake_mode(donald):
    donald.params["fake_mode"] = True
    task = donald.submit(tasks.blocking, 42)
    assert isinstance(task, asyncio.Task)
    res = await task
    assert res == 42

    from donald.queue import Queue

    donald.queue = Queue(donald)
    res = await donald.submit(tasks.blocking, 42)
    assert res == 42

    res = await donald.queue.submit(tasks.blocking, 42)
    assert res == 42


async def test_worker_cycle():
    from donald import Donald

    donald = Donald(num_workers=2, loglevel="DEBUG")
    donald.on_start(tasks.app_start)
    donald.on_stop(tasks.app_stop)

    await donald.start()

    res = await donald.submit(tasks.get_app_status)
    assert res == "STARTED"

    await donald.stop()


async def test_queue(tmp_path):
    from donald import Donald

    filepath = tmp_path / "task"
    async with Donald(num_workers=2, loglevel="DEBUG", queue=True) as donald:
        async with donald.queue as queue:
            assert donald
            assert donald._started
            assert donald.queue
            assert queue._started
            assert 42 == await donald.submit(tasks.async_, 42)

            queue.submit(tasks.write_file, str(filepath), "done")
            await asyncio.sleep(2e-2)

            queue.submit(tasks.exception)
            await asyncio.sleep(2e-2)

    assert filepath.read_text() == "done"
