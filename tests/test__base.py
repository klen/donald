import pytest

from donald import Donald
from donald.manager import TaskWrapper


def test_base():
    donald = Donald()
    assert donald
    assert donald._backend


def foo():
    return 42


async def test_wrap_task():
    donald = Donald()

    tw = donald.task()(foo)

    assert tw
    assert tw.import_path(tw._fn) == "tests.test__base.foo"
    assert isinstance(tw, TaskWrapper)
    assert tw._fn
    assert await tw() == 42

    tw = donald.task(delay=10)(foo)

    assert tw
    assert isinstance(tw, TaskWrapper)
    assert await tw() == 42
    assert tw._params["delay"] == 10


async def test_dont_wrap_local_tasks():
    donald = Donald()

    def foo():
        return 1

    with pytest.raises(AssertionError):
        donald.task()(foo)
