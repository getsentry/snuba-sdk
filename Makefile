SHELL = /bin/bash

VENV_PATH = .venv

help:
	@echo "Thanks for your interest in the Snuba SnQL SDK!"
	@echo
	@echo "make lint: Run linters"
	@echo "make tests: Run tests"
	@echo "make format: Run code formatters (destructive)"
	@echo
	@echo "Also make sure to read ./CONTRIBUTING.rst"
	@false

.venv:
	virtualenv -ppython3.8 $(VENV_PATH)
	. $(VENV_PATH)/bin/activate
	$(VENV_PATH)/bin/pip install -r test-requirements.txt
	$(VENV_PATH)/bin/pip install -r linter-requirements.txt


make install-dev: .venv
	$(VENV_PATH)/bin/pip install -r test-requirements.txt
	$(VENV_PATH)/bin/pip install -r linter-requirements.txt


setup-git:
	pip install 'pre-commit==2.16.0'
	pre-commit install --install-hooks

dist: .venv
	rm -rf dist build
	$(VENV_PATH)/bin/python setup.py sdist bdist_wheel

.PHONY: dist

format: .venv
	$(VENV_PATH)/bin/flake8 tests examples snuba_sdk
	$(VENV_PATH)/bin/black tests examples snuba_sdk
	$(VENV_PATH)/bin/isort snuba_sdk tests
	$(VENV_PATH)/bin/mypy --config-file mypy.ini --strict tests examples snuba_sdk

.PHONY: format

tests: .venv
	@$(VENV_PATH)/bin/pytest

.PHONY: tests

check: lint tests
.PHONY: check

lint: .venv
	$(VENV_PATH)/bin/flake8 tests examples snuba_sdk
	$(VENV_PATH)/bin/black --check tests examples snuba_sdk
	$(VENV_PATH)/bin/isort --df snuba_sdk tests
	$(VENV_PATH)/bin/mypy --config-file mypy.ini --strict tests examples snuba_sdk

.PHONY: lint

test-all: .venv
	tox

apidocs: .venv
	$(VENV_PATH)/bin/pip install -U -r ./docs-requirements.txt
	$(VENV_PATH)/bin/sphinx-build -W -b html docs/ docs/_build

apidocs-hotfix: apidocs
	$(VENV_PATH)/bin/pip install ghp-import
	$(VENV_PATH)/bin/ghp-import -pf docs/_build
.PHONY: apidocs-hotfix

make generate-pdocs: .venv
	$(VENV_PATH)/bin/pip install -U -r ./docs-requirements.txt
	$(VENV_PATH)/bin/pdoc -o docs/_html --search --logo "https://sentry-brand.storage.googleapis.com/sentry-wordmark-dark-280x84.png" snuba_sdk
