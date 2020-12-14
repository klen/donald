"""CLI Support."""

import asyncio
from importlib import import_module
import signal

import click

from donald import Donald


async def run_donald(donald, listen=None, module=None):
    """Start donald and import dependencies."""
    if module:
        try:
            click.echo('Import module: %s' % module)
            import_module(module)
        except ImportError:
            click.echo('Invalid module: %s' % module)
    await donald.start()


async def stop_donald(donald):
    """Stop Donald before exit."""
    await donald.stop()
    donald.loop.stop()


def handle_signal(loop, donald):
    """Stop donald before exit."""
    loop.remove_signal_handler(signal.SIGTERM)
    loop.create_task(stop_donald(donald))


@click.command()
@click.option('--listen', help="Listen the given AMPQ URL.")
@click.option('--workers', default=Donald.defaults['num_workers'], help="Number of workers.")
@click.option('--loglevel', default=Donald.defaults['loglevel'], help="Logging level.")
@click.option('--module', help="Load module.")
def main(workers=4, loglevel='info', **kw):
    """Run Donald server."""
    donald = Donald(num_workers=workers, loglevel=loglevel)

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, donald)
    loop.add_signal_handler(signal.SIGINT, handle_signal, loop, donald)
    loop.run_until_complete(run_donald(donald, **kw))
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == '__main__':
    main()
