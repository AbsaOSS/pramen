#  Copyright 2022 ABSA Group Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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

.pypi_token:
	poetry config pypi-token.pypi $(PRAMENPY_PYPI_TOKEN)

build: install
	rm -frv dist
	poetry build

test: install .env
	poetry run pytest --cov -n 1

pre-commit: install
	poetry run pre-commit run --all-files

publish: .env .pypi_token build
	poetry publish
