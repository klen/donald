Donald 2.0.1
#############

.. _description:

**Donald** — A fast and minimal task manager for **Asyncio**.


Donald supports both synchronous and asynchronous functions. It can run
coroutines across multiple event loops, schedule periodic tasks, and consume
jobs from AMQP queues.

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

.. _features:

Key Features
============

- Works with asyncio
- Simple and lightweight API
- Supports multiple backends: `memory`, `redis`, `amqp`
- Periodic task scheduling (cron or intervals)
- Built-in retry mechanism and failbacks
- Can run multiple workers and schedulers in separate processes

.. _requirements:

Requirements
=============

- Python 3.10 or newer

.. _installation:

Installation
=============

Install via pip:

::

    pip install donald

With Redis backend support:

::

    pip install donald[redis]


.. _usage:

Quick Start
===========

Initialize a task manager:

.. code:: python

    import logging
    from donald import Donald

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
    @manager.task()
    async def mytask(*args, **kwargs):
        # Do some job here

    # Start the manager somewhere (on app start for example)
    await manager.start()

    # you may run a worker in the same process
    # not recommended for production
    worker = manager.create_worker()
    worker.start()

    # ...

    # Submit the task to workers
    mytask.submit(*args, **kwargs)

    # ...

    # Stop the manager when you need
    await worker.stop()
    await manager.stop()

.. _task-tuning:

Task Tuning
===========

.. code:: python

  # Set delay and timeout
  @tasks.task(delay=5, timeout=60)
  async def delayed_task(*args, **kwargs):
      ...

  # Automatic retries on error
  @tasks.task(retries_max=3, retries_backoff_factor=2, retries_backoff_max=60)
  async def retrying_task(*args, **kwargs):
      ...

  # Define a failback function
  @retrying_task.failback()
  async def on_fail(*args, **kwargs):
      ...

  # Manual retry control
  @tasks.task(bind=True)
  async def conditional_retry(self):
      try:
          ...
      except Exception:
          if self.retries < 3:
              self.retry()
          else:
              raise

.. _scheduler:

Scheduling Tasks
================

.. code:: python

  @tasks.task()
  async def mytask(*args, **kwargs):
      ...

  # Run every 5 minutes
  mytask.schedule('*/5 * * * *')

  # Start the scheduler (not recommended in production)
  manager.scheduler.start()

  # Stop it when needed
  manager.scheduler.stop()

.. _production:

Running in Production
=====================

Create a task manager in `tasks.py`:

.. code:: python

  from donald import Donald

  manager = Donald(backend='amqp')

  # Define your tasks and schedules

Start a worker in a separate process:

.. code:: bash

   $ donald -M tasks.manager worker

Start the scheduler (optional):

.. code:: bash

   $ donald -M tasks.manager scheduler


.. _bugtracker:

Bug tracker
===========

Found a bug or have a feature request?
Please open an issue:
👉 https://github.com/klen/donald/issues

.. _contributing:

Contributing
============

Contributions are welcome!
Development happens on GitHub:
🔗 https://github.com/klen/donald

.. _license:

License
========

Licensed under a `MIT license`_.

.. _links:

.. _MIT license: http://opensource.org/licenses/MIT
.. _klen: https://klen.github.io/
