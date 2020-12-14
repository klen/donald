"""CLI Support."""

from importlib import import_module

import click

from .core import Donald, run_donald


@click.command()
@click.option('--listen', help="Listen the given AMPQ URL.")
@click.option('--workers', default=Donald.defaults['num_workers'], help="Number of workers.")
@click.option('--loglevel', default=Donald.defaults['loglevel'], help="Logging level.")
@click.option('--module', help="Load module.")
def main(workers=4, loglevel='info', module=None, **kw):
    """Run Donald server."""
    donald = Donald(num_workers=workers, loglevel=loglevel)

    if module:
        try:
            click.echo('Import module: %s' % module)
            import_module(module)
        except ImportError:
            click.echo('Invalid module: %s' % module)

    run_donald(donald)


if __name__ == '__main__':
    main()
