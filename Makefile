.PHONY: help
# target: help - Display callable targets
help:
	@egrep "^# target:" [Mm]akefile

.PHONY: clean
# target: clean - Clean the repository
clean:
	rm -rf build/ dist/ docs/_build *.egg-info .venv .tox
	find $(CURDIR) -name "*.py[co]" -delete
	find $(CURDIR) -name "*.orig" -delete
	find $(CURDIR) -name "__pycache__" -delete

# =============
#  Development
# =============

.PHONY: install
# target: install - Install project dependencies via uv
install:
	uv sync --extra tests --extra dev
	uv run pre-commit install

.PHONY: run
# target: run - Run test.py via uv
run:
	uv run python test.py

.PHONY: rabbit
# target: rabbit - Start a local RabbitMQ container for AMQP tests
rabbit:
	docker run --name rabbit --rm -p 15672:15672 -p 5672:5672 rabbitmq:3-management

.PHONY: t test
# target: test - Runs tests
t test:
	docker start rabbitmq || true
	@uv run pytest tests

.PHONY: types
# target: types - Run pyrefly type checker
types:
	uv run pyrefly check $(CURDIR)/donald

.PHONY: example
# target: example - Run example worker via uv
example:
	uv run python -m donald -M example.manager worker -S

# ==============
#  Bump version
# ==============

.PHONY: release
VERSION?=minor
# target: release - Bump version
release:
	@uv run bump2version $(VERSION)
	@git checkout main
	@git merge develop
	@git checkout develop
	@git push --all
	@git push --tags

.PHONY: minor
minor: release

.PHONY: patch
patch:
	make release VERSION=patch

.PHONY: major
major:
	make release VERSION=major
