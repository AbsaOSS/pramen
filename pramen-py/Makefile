ENV ?= local

ifneq (,$(wildcard ./.env.$(ENV)))
	include .env.$(ENV)
endif

install: .poetry_installed

.poetry_installed:
	poetry install
	touch $@

.env:
	ln -sfv .env.$(ENV) .env

build: install
	rm -frv dist
	poetry build

test: install .env
	poetry run pytest --cov

pre-commit: install
	poetry run pre-commit run --all-files

publish: .env build
	poetry publish
