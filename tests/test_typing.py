"""Type-level checks. Enforced by pyrefly (see [tool.pyrefly] project-includes)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from asyncio import Task
    from typing import Any, Coroutine, assert_type

    from donald import Donald
    from donald.tasks import TaskRun, TaskWrapper

    manager = Donald(log_level="DEBUG", worker_params={"max_tasks": 2})

    @manager.task()
    async def async_task(x: int, *, y: str = "") -> int:
        return x

    @manager.task()
    def sync_task(x: int) -> str:
        return str(x)

    # The decorator keeps the signature and the result type of the wrapped function.
    assert_type(async_task(1, y="a"), Coroutine[Any, Any, int])
    assert_type(sync_task(1), Coroutine[Any, Any, str])

    # The parameters survive scheduling and submitting.
    wrapper = async_task
    assert_type(async_task.schedule(60)(1, y="a"), Coroutine[Any, Any, int])
    assert_type(wrapper.submit(1), Task[Any])
    assert_type(wrapper.apply_submit(1), Task[Any])
    assert_type(wrapper.get_run(1, kwargs={}), TaskRun)

    # Manager accepts a TaskRun or a callable.
    assert_type(manager.submit(async_task, 1), Task[Any])
    assert_type(manager.submit_and_wait(async_task, 1), Coroutine[Any, Any, Any])
