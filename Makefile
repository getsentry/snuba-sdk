SHELL = /bin/bash

VENV_PATH = .venv

help:
	@echo "Thanks for your interest in the Snuba SnQL SDK!"
	@echo
	@echo "make lint: Run linters"
	@echo "make tests: Run tests"
	@echo "make format: Run code formatters (destructive)"
	@echo
	@echo "Also make sure to read ./CONTRIBUTING.md"
	@false

.venv:
	virtualenv -ppython3.9 $(VENV_PATH)
	$(VENV_PATH)/bin/pip install tox

setup-git:
	pip install 'pre-commit==2.9.3'
	pre-commit install --install-hooks

dist: .venv
	rm -rf dist build
	$(VENV_PATH)/bin/python setup.py sdist bdist_wheel

.PHONY: dist

format: .venv
	$(VENV_PATH)/bin/tox -e linters --notest
	.tox/linters/bin/black .
.PHONY: format

tests: .venv
	@$(VENV_PATH)/bin/tox -e py3.9
.PHONY: tests

check: lint tests
.PHONY: check

lint: .venv
	@set -e && $(VENV_PATH)/bin/tox -e linters || ( \
		echo "================================"; \
		echo "Bad formatting? Run: make format"; \
		echo "================================"; \
		false)

.PHONY: lint

install-zeus-cli:
	npm install -g @zeus-ci/cli
.PHONY: install-zeus-cli

travis-upload-dist: dist install-zeus-cli
	zeus upload -t "application/zip+wheel" dist/* \
		|| [[ ! "$(TRAVIS_BRANCH)" =~ ^release/ ]]
.PHONY: travis-upload-dist
