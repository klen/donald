VIRTUAL_ENV ?= $(CURDIR)/env


$(VIRTUAL_ENV): $(CURDIR)/requirements-dev.txt
	$(VIRTUAL_ENV)/bin/pip install -r requirements-dev.txt


run: $(VIRTUAL_ENV)
	$(VIRTUAL_ENV)/bin/python test.py

rabbit:
	docker run --name rabbit --rm -p 15672:15672 -p 5672:5672 rabbitmq:3-management
