VIRTUAL_ENV ?= $(CURDIR)/.venv

all: $(VIRTUAL_ENV)

.PHONY: help
# target: help - Display callable targets
help:
	@egrep "^# target:" [Mm]akefile

.PHONY: clean
# target: clean - Clean the repository
clean:
	rm -rf build/ dist/ docs/_build *.egg-info
	find $(CURDIR) -name "*.py[co]" -delete
	find $(CURDIR) -name "*.orig" -delete
	find $(CURDIR)/$(MODULE) -name "__pycache__" -delete

# ==============
#  Bump version
# ==============

.PHONY: release
VERSION?=minor
# target: release - Bump version
release: $(VIRTUAL_ENV)
	@$(VIRTUAL_ENV)/bin/bump2version $(VERSION)
	@git checkout master
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

# =============
#  Development
# =============

$(VIRTUAL_ENV): pyproject.toml
	python -m venv $(VIRTUAL_ENV)
	$(VIRTUAL_ENV)/bin/pip install -e .[tests,dev]
	$(VIRTUAL_ENV)/bin/pre-commit install
	touch $(VIRTUAL_ENV)


run: $(VIRTUAL_ENV)
	$(VIRTUAL_ENV)/bin/python test.py

rabbit:
	docker run --name rabbit --rm -p 15672:15672 -p 5672:5672 rabbitmq:3-management

.PHONY: t test
# target: test - Runs tests
t test: $(VIRTUAL_ENV)
	docker start rabbitmq
	@$(VIRTUAL_ENV)/bin/pytest tests

mypy: $(VIRTUAL_ENV)
	mypy donald

example: $(VIRTUAL_ENV)
	$(VIRTUAL_ENV)/bin/python -m donald -M example.manager worker -S
