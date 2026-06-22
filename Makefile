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

RELEASE	?= minor
MANAGER	?= uv

.PHONY: release
# target: release - Bump version
release:
	@echo "Starting release process (bumping $(RELEASE) version)..."
	@git checkout main
	@git pull
	@git checkout develop
	@git pull
	@echo "Bumping version and creating release commit and tag..."
	@uvx bump-my-version bump $(RELEASE)
	@echo "Version bumped to `$(MANAGER) version --short`."
	@$(MANAGER) lock
	@echo "Committing version bump and creating tag..."
	@VERSION=`$(MANAGER) version --short`; \
		{ \
			printf 'build(release): %s\n\n' "$$VERSION"; \
			printf 'Changes:\n\n'; \
			git log --oneline --pretty=format:'%s [%an]' main..develop | grep -Evi 'github|^Merge' || true; \
		} | git commit -a -F -
	@echo "Merging changes between branches..."
	@git checkout main
	@git merge --ff-only develop
	@VERSION=`$(MANAGER) version --short`; \
		git push origin main; \
		git tag -a "$$VERSION" -m "$$VERSION"; \
		git push origin "$$VERSION"
	@git checkout develop
	@git merge --ff-only main
	@git push origin develop
	@echo "Release process complete for `$(MANAGER) version --short`"

.PHONY: minor
minor: release

.PHONY: patch
patch:
	make release RELEASE=patch

.PHONY: major
major:
	make release RELEASE=major

version v:
	uv version --short
