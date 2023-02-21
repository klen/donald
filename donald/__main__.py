from __future__ import annotations

import asyncio
import signal
from functools import wraps
from typing import Awaitable, Callable

import click

from .types import TV
from .utils import import_obj


def import_manager(path: str) -> Donald:
    """Import a manager from a python path."""
    manager: Donald = import_obj(path)
    return manager


def process_await(fn: Callable[..., Awaitable[TV]]) -> Callable[..., TV]:
    @wraps(fn)
    @click.pass_context
    def wrapper(ctx, *args, **kwargs):
        loop = ctx.obj["loop"]
        return loop.run_until_complete(fn(ctx, *args, **kwargs))

    return wrapper


@click.group()
@click.option(
    "-M", "--manager", "manager", required=True, help="Python path to the manager"
)
@click.pass_context
def cli(ctx: click.Context, manager: str):
    ctx.obj["manager"] = import_manager(manager)


@cli.command(help="Launch a worker")
@click.option("-S", "--scheduler", "scheduler", is_flag=True, help="Start a scheduler")
@process_await
async def worker(ctx: click.Context, scheduler: bool = False, **params):
    """Launch a worker."""

    loop = ctx.obj["loop"]

    async def stop():
        loop.remove_signal_handler(signal.SIGTERM)
        loop.remove_signal_handler(signal.SIGINT)
        await worker.stop()
        if scheduler:
            await manager.scheduler.stop()
        await manager.stop()

    loop.add_signal_handler(signal.SIGINT, lambda: loop.create_task(stop()))
    loop.add_signal_handler(signal.SIGTERM, lambda: loop.create_task(stop()))

    manager: Donald = ctx.obj["manager"]
    await manager.start()
    if scheduler:
        manager.scheduler.start()

    worker = manager.create_worker(show_banner=True, **params)
    worker.start()

    await worker.wait()


@cli.command(help="Launch a scheduler")
@process_await
async def scheduler(ctx: click.Context, **params):

    loop = ctx.obj["loop"]

    async def stop():
        loop.remove_signal_handler(signal.SIGTERM)
        loop.remove_signal_handler(signal.SIGINT)
        await manager.scheduler.stop()
        await manager.stop()

    loop.add_signal_handler(signal.SIGINT, lambda: loop.create_task(stop()))
    loop.add_signal_handler(signal.SIGTERM, lambda: loop.create_task(stop()))

    manager: Donald = ctx.obj["manager"]
    await manager.start()

    manager.scheduler.start()
    await manager.scheduler.wait()


def main():
    loop = asyncio.get_event_loop()
    cli(obj={"loop": loop})


if __name__ == "__main__":
    main()

from donald.manager import Donald
