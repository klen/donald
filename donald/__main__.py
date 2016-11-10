import click
import asyncio
from donald import Donald
from importlib import import_module


@asyncio.coroutine
def run_donald(donald, listen=None, module=None):
    yield from donald.start()
    if module:
        try:
            click.echo('Import module: %s' % module)
            import_module(module)
        except ImportError:
            click.echo('Invalid module: %s' % module)


@click.command()
@click.option('--listen', help="Listen the given AMPQ URL.")
@click.option('--workers', default=4, help="Number of workers.")
@click.option('--loglevel', default='INFO', help="Logging level.")
@click.option('--module', help="Load module.")
def main(workers=4, loglevel='info', **kw):
    """Run Donald server."""
    donald = Donald(num_threads=workers, loglevel=loglevel)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_donald(donald, **kw))
    loop.run_forever()


if __name__ == '__main__':
    main()
