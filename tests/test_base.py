from donald import Donald
from donald.manager import TaskWrapper


def test_base():
    donald = Donald()
    assert donald
    assert donald._backend


def test_wrap_task():
    donald = Donald()

    @donald.task
    def foo():
        return 1

    assert foo
    assert isinstance(foo, TaskWrapper)
    assert foo._fn
    assert foo._fn() == 1

    @donald.task(delay=10)
    def bar():
        return 2

    assert bar
    assert isinstance(bar, TaskWrapper)
    assert bar._fn() == 2
    assert bar._params["delay"] == 10
