Donald
######

.. _description:

Donald -- A simple task engine for Asyncio.

The main goal for Donald to run async/sync code without blocking main loop.

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

.. _usage:

Usage
=====

From shell: ::

    $ donald --help


From asynchronous python code:

.. code:: python

    # Init Donald
    donald = Donald(
        # Params (default values)
        # -----------------------

        # Run tasks imediatelly in the same process/thread
        fake_mode=False,

        # Number of workers
        num_workers=multiprocessing.cpu_count() - 1,

        # Maximum concurent tasks per worker
        max_tasks_per_worker=100,

        # Ensure that the Donald starts only once (set to filename to lock)
        filelock=None,

        # logging level
        loglevel='INFO',

        # AMQP params
        queue={
            'exchange': 'donald',
            'queue': 'donald',
        }
    )

    # Schedule periodic tasks
    @donald.schedule(crontab_string | seconds_float | datetime_timedelta, *args, **kwargs)
    async def task(*args, **kwargs):
        # ...

    # Start the donald
    await donald.start()

    # ...

    # Submit a task to donald
    await donald.submit(corofunction or function, *args, **kwargs)

    # Submit and wait for result
    result = await donald.submit(corofunction or function, *args, **kwargs)

    # ...

    # Stop the donald
    await donald.stop()

Connect and receive tasks using AMQP
------------------------------------

.. code:: python

    donald = Donald()

    await donald.start()
    await donald.queue.start()

    # ...


    # Stop the donald
    await donald.queue.stop()
    await donald.stop()

Submit tasks to AMQP
--------------------

.. code::

    # Send task to queue
    await donald.queue.submit(<coro or func>, *args, **kwargs)

    # ...

    # Listen tasks
    await donald.queue.listen()
    await donald.listen(<AMQP URL>)


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


Contributors
=============

* klen_ (Kirill Klenov)

.. _license:

License
========

Licensed under a `BSD license`_.

.. _links:

.. _BSD license: http://www.linfo.org/bsdlicense.html
.. _klen: https://klen.github.io/
