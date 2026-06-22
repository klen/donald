Donald is a Python asyncio iask manager with sync/async tasks, multiple backends (`memory`, `redis`, `amqp`), scheduling, retries, and workers.

- **Python:** 3.11+
- **Package:** `donald/` configured in `pyproject.toml`
- **CLI:** `donald` console script
- **Tests:** `tests/` using pytest
- **Docs:** `README.rst`
- **Venv:** `.venv/` managed by `Makefile`

## Key files

- `donald/manager.py` — core `Donald` manager
- `donald/tasks.py` — task wrapper/run logic
- `donald/worker.py` / `scheduler.py` — workers and scheduling
- `donald/backend.py` — backend registry (`memory`, `redis`, `amqp`)
- `donald/types.py` — type aliases
- `tests/conftest.py` — shared fixtures
- `pyproject.toml` — project config and tool settings

## Safe validation

```bash
.venv/bin/ruff check donald
.venv/bin/ruff format donald
.venv/bin/pyrefly check donald
.venv/bin/pytest tests
```

## Editing guidelines

- Keep line length ≤ 100, target Python 3.10+.
- Run ruff and pyrefly after source changes.
- Preserve `donald/__init__.py` exports.
- Follow existing test patterns and fixtures.

## High-risk commands

Avoid unless explicitly requested:

- `make release/minor/major/patch` — version bump + merge + push + potential PyPI release.
- `git push --tags` / `git push --all` / merging to `master`.
- `docker start rabbitmq` — needs local `rabbitmq` container.
- Hand-editing version strings in `pyproject.toml` / `README.rst`.

## Files and directories to avoid

- `donald_old/` — legacy reference code.
- Generated files: `donald.egg-info/`, `.tox/`, `.pytest_cache/`, `.ruff_cache/`, `__pycache__/`.
- `.venv/` — managed environment.
- `.github/workflows/` — CI/CD; edit with care.
- `example.py` / `example.sh` — examples only.

## Repo-specific notes

- Work on `develop`; `master` is for releases.
- Pre-commit runs ruff, pyrefly, and pytest on push.
- Backends are registered in `donald/backend.py` via `BACKENDS`.
- Do not commit, push, or release without explicit user approval.
