# Donald

[![Tests Status](https://github.com/klen/donald/workflows/tests/badge.svg)](https://github.com/klen/donald/actions)
[![PYPI Version](https://img.shields.io/pypi/v/donald)](https://pypi.org/project/donald/)
[![Python Versions](https://img.shields.io/pypi/pyversions/donald)](https://pypi.org/project/donald/)

**Donald** is a small task manager for asyncio. You wrap a function into a task,
submit it, and a worker picks it up from a backend queue. The same wrapper gives
you retries, timeouts, cron schedules and an optional RPC call that waits for
the result.

Tasks are referenced by import path, not by reference. A worker in another
process imports the task module and calls the function there.

- Backends: `memory`, `redis`, `amqp`
- Sync and async functions
- Delays, timeouts, retries with backoff, failbacks
- Periodic and cron schedules
- Multiple workers and schedulers in separate processes
- Built-in healthchecks for workers and the scheduler

## Requirements

- Python 3.11 or newer

## Installation

```bash
pip install donald           # includes the AMQP backend (aio-pika)
pip install donald[redis]    # adds the redis backend
```

## Quick Start

```python
import asyncio

from donald import Donald

manager = Donald(
    log_level="INFO",          # level name, applied to the "donald" logger
    log_config=None,           # dict for logging.config.dictConfig

    backend="memory",          # memory | redis | amqp
    backend_params={},         # see "Backends" below

    worker_params={
        "max_tasks": 0,        # 0 means no limit
        "task_defaults": {},   # fallback task params (see note below)
        "on_start": None,      # awaitable, called on worker start
        "on_stop": None,       # awaitable, called on worker stop
        "on_error": None,      # awaitable(exc), called on task failure
    },

    scheduler_params={
        "heartbeat_file": "/tmp/donald-scheduler.heartbeat",
        "heartbeat_interval": 60,  # seconds, 0 disables the heartbeat
    },
)


@manager.task()
async def mytask(value):
    return value * 2


async def main():
    await manager.start()

    # A worker in the same process. Production setups run it in another process.
    worker = manager.create_worker()
    worker.start()

    mytask.submit(21)                      # fire and forget
    result = await mytask.submit_and_wait(21)  # RPC, needs a running worker
    assert result == 42

    await worker.stop()
    await manager.stop()


asyncio.run(main())
```

`manager.start()` connects the backend. Submitting before that raises
`ManagerNotReadyError`. `manager.stop()` also stops the scheduler.

For short scripts there are two shortcuts:

```python
manager = await Donald.create(backend="memory")  # construct and start

async with Donald(backend="memory") as manager:
    ...
```

## Defining Tasks

The decorator must be called with parentheses; `@manager.task` without them
raises `ValueError`.

```python
@manager.task()
def sync_task(x):        # sync functions run in the loop, not in a thread
    return x + 1


@manager.task(delay=5, timeout=60)
async def slow_task(x):
    ...
```

Local functions cannot be tasks: the worker imports the task by
`module.qualname`, so the function must be defined at module level.

Task parameters, all optional:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `delay` | `0` | Seconds to sleep before running. Reapplied before each retry |
| `timeout` | `0` | Seconds before the task is cancelled. `0` means no timeout |
| `retries_max` | `0` | Retries after a failure. Must be between 0 and 100 |
| `retries_backoff_factor` | `0` | Backoff base. `0` means retry immediately |
| `retries_backoff_max` | `600` | Cap for the backoff delay in seconds |
| `bind` | `False` | Pass the `TaskRun` as the first argument |

Ways to run a task:

```python
await mytask(1)                  # call in place, no queue
mytask.submit(1)                 # queue it, returns an asyncio.Task for the submit
await mytask.submit_and_wait(1)  # queue it and wait for the result
mytask.apply_submit(1, delay=10) # queue it with per-call params
```

`submit_and_wait` and `manager.submit_and_wait(fn, ...)` use a reply channel, so
a worker has to be consuming tasks. The timeout defaults to 10 seconds and
raises `TimeoutError` when nothing comes back.

A plain function can be submitted without wrapping it, as long as it is
importable:

```python
manager.submit(some_module.some_function, 1, 2)
```

## Retries and Failback

```python
@manager.task(retries_max=3, retries_backoff_factor=2, retries_backoff_max=60)
async def flaky():
    ...
```

After a failure with `n` retries done, the next attempt waits
`min(retries_backoff_max, retries_backoff_factor * (2 ** (n - 1) + random()))`
seconds. A zero factor keeps retries immediate.

When retries are exhausted, a failback gets the exception and the original
arguments. Without a failback the exception is logged by the worker and passed
to `on_error`:

```python
@flaky.failback()
async def on_fail(exc, *args, **kwargs):
    logger.warning("Giving up: %s", exc)
```

To decide about a retry inside the task, bind it and call `retry()`:

```python
@manager.task(bind=True)
async def maybe_retry(self, payload):
    if payload == "retry-me":
        return self.retry()  # submits a new run, returns its asyncio.Task

    ...
```

`self.retries` is the number of retries already made for this run. Calling
`retry()` resubmits the same run with the incremented counter.

## Scheduling

An interval can be seconds (`int` or `float`), a `timedelta`, a cron string or
a `crontab.CronTab` instance. Cron expressions are evaluated in UTC.

```python
@manager.schedule("*/5 * * * *")
@manager.task()
async def every_five_minutes():
    ...


@manager.schedule(30, run_immediately=True)
@manager.task()
async def every_thirty_seconds():
    ...
```

`mytask.schedule(interval)` works too and is applied immediately.

The scheduler is a plain object on the manager:

```python
manager.scheduler.start()   # sync, starts the schedule loops and heartbeat
await manager.scheduler.stop()
await manager.scheduler.wait()  # blocks until stopped (used by the CLI)
```

Schedules start counting after `start()`. The first run happens after the
interval unless `run_immediately=True`.

Run one scheduler only. Starting it both inside a worker (`donald worker -S`)
and as a separate process submits every scheduled task twice.

## Workers

Each worker subscribes to the backend and runs tasks concurrently:

```python
worker = manager.create_worker(max_tasks=10)
worker.start()
await worker.wait()   # until stopped or the worker fails
await worker.stop()   # cancels running tasks, calls on_stop
```

Hooks can be set in `worker_params` or registered on the manager:

```python
@manager.on_start
async def connect_db():
    ...


@manager.on_stop
async def close_db():
    ...


@manager.on_error
async def report(exc):
    ...
```

`on_error` receives the exception of a failed task.

`worker_params["task_defaults"]` fills in task params that a run does not
carry. Task wrappers always carry all params, so in practice it only applies to
runs submitted with partial params.

## Healthchecks

Worker healthcheck submits a `ping` task over the backend and waits for
`pong`. It returns `False` on timeout instead of raising:

```python
healthy = await manager.healthcheck(timeout=10)
```

Scheduler healthcheck reads the heartbeat file written by the scheduler. It
checks that the PID in the file is alive and that the last heartbeat is not
older than `heartbeat_interval * 2` seconds:

```python
healthy = await manager.scheduler_healthcheck()
```

Both work from another process, which is what a Docker `HEALTHCHECK` needs:

```dockerfile
HEALTHCHECK --interval=30s --retries=3 \
  CMD python -c "import asyncio; from tasks import manager; exit(0 if asyncio.run(manager.scheduler_healthcheck()) else 1)"
```

The heartbeat file lives on the local filesystem, so this check only makes
sense when the healthcheck runs on the same host or in the same container as
the scheduler. Set `heartbeat_interval=0` to disable the file.

## Backends

`memory` keeps tasks in an `asyncio.Queue` inside the current process. It is
meant for tests and single-process apps; a worker in another process sees
nothing, and queued tasks are lost on restart.

`redis` uses Redis streams with a consumer group. It requires the `redis`
extra:

```python
Donald(
    backend="redis",
    backend_params={
        "url": "redis://localhost:6379/0",
        "channel": "donald",       # stream and group prefix
        "consumer": "consumer-1",  # unique per worker
    },
)
```

`amqp` publishes to a RabbitMQ queue and is included by default:

```python
Donald(
    backend="amqp",
    backend_params={
        "url": "amqp://guest:guest@localhost:5672/",
        "queue": "donald",   # also used as the routing key
        "exchange": "",      # empty uses the default exchange
    },
)
```

Task payloads (`args`, `kwargs`) are pickled, so everything passed to a task
must be picklable. RPC results are pickled as well.

## Running in Production

Create a manager with tasks in `tasks.py`:

```python
from donald import Donald

manager = Donald(backend="amqp")


@manager.task()
async def send_email(user_id):
    ...
```

Start a worker in a separate process:

```bash
donald -M tasks.manager worker
```

Run a worker with the scheduler in the same process:

```bash
donald -M tasks.manager worker -S
```

Or run the scheduler alone:

```bash
donald -M tasks.manager scheduler
```

`-M/--manager` is required and takes the import path of the manager object.
The CLI starts the manager, handles `SIGINT`/`SIGTERM` by stopping the worker,
the scheduler and the manager, and then exits. The scheduler command blocks
until it is stopped.

The worker process imports tasks from `tasks.py`, so the module must be
importable there (same working directory, same package installed). Creating
the manager at import time is fine; do not call `manager.start()` at import
time.

## Limitations

- The `memory` backend cannot be shared between processes.
- Tasks must be module-level functions so a worker can import them by path.
- Arguments and RPC results go through `pickle`.
- The scheduler's cron support comes from `crontab` and evaluates in UTC.
- A schedule lives in the process that started it; `heartbeat_file` only
  reports liveness, it does not coordinate schedulers.

## Public API

`Donald`: `create`, `start`, `stop`, `task`, `schedule`, `submit`,
`submit_and_wait`, `create_worker`, `on_start`, `on_stop`, `on_error`,
`healthcheck`, `scheduler_healthcheck`, `scheduler`.

`TaskWrapper`: `submit`, `submit_and_wait`, `apply_submit`, `schedule`,
`failback`, plus a direct call that runs the function in place.

`TaskRun`: `retries`, `retry`.

## Bug tracker

Found a bug or have a feature request? Open an issue:
https://github.com/klen/donald/issues

## Contributing

Contributions are welcome. Development happens on GitHub:
https://github.com/klen/donald

## License

Licensed under a [MIT license](http://opensource.org/licenses/MIT).
