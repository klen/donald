Donald 0.31.9
#############

.. _description:

Donald -- A fast and simple tasks manager for Asyncio.


Donald supports synchronous and asynchronous paradigms. The package is running
coroutines and functions in multi loops. Donald could run periodic tasks and
listen AMQP queues.

.. _badges:

.. image:: https://github.com/klen/donald/workflows/tests/badge.svg
    :target: https://github.com/klen/donald/actions
    :alt: Tests Status

.. image:: https://img.shields.io/pypi/v/donald
    :target: https://pypi.org/project/donald/
    :alt: PYPI Version

.. image:: https://img.shields.io/pypi/pyversions/donald
    :target: https://pypi.org/project/donald/
    :alt: Python Versions

.. _contents:

.. contents::

.. _requirements:

Requirements
=============

- python 3.8+

.. _installation:

Installation
=============

**Donald** should be installed using pip: ::

    pip install donald

With redis support: ::

    pip install donald[redis]

.. _usage:

Quick Start
===========

Init the tasks manager:

.. code:: python

    # Init Donald
    manager = Donald(

        # Params (default values)
        # -----------------------

        # Setup logging
        log_level=logging.INFO,
        log_config=None,

        # Choose a backend (memory|redis|amqp)
        # memory - is only recommended for testing/local development
        backend='memory',

        # Backend connection params
        # redis: {'url': 'redis://localhost:6379/0', 'channel': 'donald'}
        # amqp: {'url': 'amqp://guest:guest@localhost:5672/', 'queue': 'donald', 'exchange': 'donald'}
        backend_params={},

        # Tasks worker params
        worker_params={
          # Max tasks in work
          'max_tasks': 0,

          # Tasks default params (delay, timeout)
          'task_defaults': {},

          # A awaitable function to run on worker start
          'on_start': None

          # A awaitable function to run on worker stop
          'on_stop': None

          # A awaitable function to run on worker error
          'on_error': None

        },
    )

    # Wrap a function to task
    @manager.task
    async def myfunc(*args, **kwargs):
        # Do some job here

    # Start the manager somewhere (on app start for example)
    await manager.start()

    # you may run a worker in the same process
    # not recommended for production
    worker = manager.create_worker()
    worker.start()

    # ...

    # Submit the task to workers
    myfunc.submit(*args, **kwargs)

    # ...

    # Stop the manager when you need
    await worker.stop()
    await manager.stop()


Schedule tasks
===============

.. code:: python

  @tasks.schedule('*/5 * * * *')  # Supports cron expressions, number of seconds, timedelta
  @tasks.task
  async def myfunc(*args, **kwargs):
      """Run every 5 minutes"""
      # Do some job here


  # you may run a scheduler in the same process
  # not recommended for production
  manager.scheduler.start()

  # ...

  # Stop the scheduler before stop the tasks manager
  manager.scheduler.stop()


Run in production
=================

Create a tasks manager somewhere in your app `tasks.py`:

.. code:: python

  manager = Donald(backend='amqp')

  # Setup your tasks and schedules.
  # See the Quick Start section for details.

Run a worker in a separate process:

.. code:: bash

   $ donald -M tasks.manager worker

Run a scheduler (if you need) in a separate process:

.. code:: bash

   $ donald -M tasks.manager scheduler

.. _bugtracker:

Bug tracker
===========

If you have any suggestions, bug reports or
annoyances please report them to the issue tracker
at https://github.com/klen/donald/issues

.. _contributing:

Contributing
============

Development of starter happens at github: https://github.com/klen/donald

.. _license:

License
========

Licensed under a `BSD license`_.

.. _links:

.. _BSD license: http://www.linfo.org/bsdlicense.html
.. _klen: https://klen.github.io/
