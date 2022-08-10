DEPLOY_ENV ?= local

ifneq (,$(wildcard ./.env.$(DEPLOY_ENV)))
	include .env.$(DEPLOY_ENV)
endif

PY_PACKAGE_NAME ?= syncwatcher-py-examples
PIP ?= $(DEPLOY_ENV_VENV_PYTHON) -m pip

COMMIT_SHA ?= $(shell git rev-parse --verify --short HEAD)
VERSION ?= $(shell sed -rn "s/^version = \"(.*)\"/\1/p" pyproject.toml)
INSTALL_PKG_CMD = \
	$(PIP) uninstall -y $(PY_PACKAGE_NAME); \
	$(PIP) install --no-cache-dir --index-url \
	$(JFROG_PYPI_INSTALL_URL) $(PY_PACKAGE_NAME)==$(VERSION)

install: .add_jfrog_repo .poetry_installed

.poetry_installed:
	poetry install
	touch $@

.env:
	ln -sfv .env.$(DEPLOY_ENV) .env

.add_jfrog_repo:
	poetry config repositories.jfrog $(JFROG_PYPI_PUBLISH_URL)
	poetry config http-basic.jfrog $(POETRY_HTTP_BASIC_JFROG_USERNAME) $(POETRY_HTTP_BASIC_JFROG_PASSWORD)
	touch $@

build: install
	rm -frv dist
	poetry build

test: install
	poetry run pytest --cov

pre-commit: install
	poetry run pre-commit run --all-files

publish: .env .add_jfrog_repo build
	poetry publish --repository jfrog

release: CMD = $(INSTALL_PKG_CMD)
release: execute_on_deploy_host

rollback: CMD = $(INSTALL_PKG_CMD)
rollback: execute_on_deploy_host

release-all:
	DEPLOY_ENV=asgard make release
	DEPLOY_ENV=chewbacca make release
	DEPLOY_ENV=binks make release
	DEPLOY_ENV=valhalla make release

execute_on_deploy_host:
	ssh -i $(DEPLOY_ENV_IDENTITY_FILE) $(DEPLOY_ENV_USER)@$(DEPLOY_ENV_HOSTNAME) "$(CMD)"

rsync_to_deploy_host:
	rsync -avupPtz -e 'ssh -i $(DEPLOY_ENV_IDENTITY_FILE)' $(LOCAL_SOURCE) $(DEPLOY_ENV_USER)@$(DEPLOY_ENV_HOSTNAME):$(REMOTE_TARGET)
