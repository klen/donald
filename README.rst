Donald
######

.. _description:

Donald -- A simple task engine for Asyncio.

The main goal for Donald to run async/sync code without blocking main loop.

Donald supports synchronous and asynchronous paradigms. The package is running
coroutines and functions in multi loops. Donald could run periodic tasks and
listen AMQP queues.

.. _badges:

.. image:: http://img.shields.io/travis/klen/donald.svg?style=flat-square
    :target: http://travis-ci.org/klen/donald
    :alt: Build Status

.. image:: http://img.shields.io/coveralls/klen/donald.svg?style=flat-square
    :target: https://coveralls.io/r/klen/pewee_migrate
    :alt: Coverals

.. image:: http://img.shields.io/pypi/v/donald.svg?style=flat-square
    :target: https://pypi.python.org/pypi/donald
    :alt: Version

.. image:: http://img.shields.io/pypi/dm/donald.svg?style=flat-square
    :target: https://pypi.python.org/pypi/donald
    :alt: Downloads

.. _contents:

.. contents::

.. _requirements:

Requirements
=============

- python 3.3+

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


From synchronous python code: ::

    donald = Donald()
    donald.start()

    donald.submit(<coro or function>)
    donald.schedule(<seconds>, <coro or function>)


From asynchronous python code: ::

    donald = Donald()

    await donald.start()
    result = await donald.submit(<coro or function>)
    await donald.schedule(<seconds>, <coro or function>)

Listen AMQP
-----------

AMQP: ::

    donald = Donald()

    await donald.start()

    # Send task to queue
    await donald.queue.start(False)
    await donald.queue.submit(<coro or func>, *args, **kwargs)

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
=======

Licensed under a `BSD license`_.

.. _links:

.. _BSD license: http://www.linfo.org/bsdlicense.html
.. _klen: https://klen.github.io/
.. _Flask: http://flask.pocoo.org/
.. _Flask-PW: https://github.com/klen/flask-pw
