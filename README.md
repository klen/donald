# Donald

[![Tests Status](https://github.com/klen/donald/workflows/tests/badge.svg)](https://github.com/klen/donald/actions)
[![PYPI Version](https://img.shields.io/pypi/v/donald)](https://pypi.org/project/donald/)
[![Python Versions](https://img.shields.io/pypi/pyversions/donald)](https://pypi.org/project/donald/)

**Donald** — A fast and minimal task manager for **Asyncio**.

Donald supports both synchronous and asynchronous functions. It can run
coroutines across multiple event loops, schedule periodic tasks, and consume
jobs from AMQP queues.

## Key Features

- Works with asyncio
- Simple and lightweight API
- Supports multiple backends: `memory`, `redis`, `amqp`
- Periodic task scheduling (cron or intervals)
- Built-in retry mechanism and failbacks
- Can run multiple workers and schedulers in separate processes

## Requirements

- Python 3.11 or newer

## Installation

Install via pip:

```bash
pip install donald
```

With Redis backend support:

```bash
pip install donald[redis]
```

## Quick Start

Initialize a task manager:

```python
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

    # Scheduler params
    scheduler_params={
      # Heartbeat file for scheduler healthcheck (cross-process)
      'heartbeat_file': '/tmp/donald-scheduler.heartbeat',

      # How often to update the heartbeat file (seconds)
      'heartbeat_interval': 60,
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
```

## Task Tuning

```python
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
```

## Scheduling Tasks

```python
@tasks.task()
async def mytask(*args, **kwargs):
    ...

# Run every 5 minutes
mytask.schedule('*/5 * * * *')

# Start the scheduler (not recommended in production)
manager.scheduler.start()

# Stop it when needed
manager.scheduler.stop()
```

## Healthchecks

Donald provides two healthcheck methods for monitoring:

**Worker healthcheck** — submits a ping task and waits for a worker to respond:

```python
# Returns True if a worker is alive and processing tasks
healthy = await manager.healthcheck(timeout=10)
```

**Scheduler healthcheck** — reads the heartbeat file written by the scheduler:

```python
# Returns True if the scheduler process is alive and heartbeat is fresh
healthy = await manager.scheduler_healthcheck()
```

The scheduler writes PID and a timestamp to a heartbeat file at a
configurable interval. The healthcheck reads this file and verifies:

- The PID corresponds to a running process (via `os.kill(pid, 0)`)
- The last heartbeat timestamp is within `heartbeat_interval * 2` seconds

This works cross-process, making it suitable for Docker `HEALTHCHECK`
instructions regardless of the backend in use.

**Docker example:**

```dockerfile
HEALTHCHECK --interval=30s --retries=3 \
  CMD python -c "import asyncio; from tasks import manager; exit(0 if asyncio.run(manager.scheduler_healthcheck()) else 1)"
```

To disable the heartbeat (e.g. for testing), set `heartbeat_interval=0`.

## Running in Production

Create a task manager in `tasks.py`:

```python
from donald import Donald

manager = Donald(backend='amqp')

# Define your tasks and schedules
```

Start a worker in a separate process:

```bash
$ donald -M tasks.manager worker
```

Start the scheduler (optional):

```bash
$ donald -M tasks.manager scheduler
```

## Bug tracker

Found a bug or have a feature request?
Please open an issue:
👉 https://github.com/klen/donald/issues

## Contributing

Contributions are welcome!
Development happens on GitHub:
🔗 https://github.com/klen/donald

## License

Licensed under a [MIT license](http://opensource.org/licenses/MIT).
