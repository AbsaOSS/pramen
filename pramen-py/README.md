# Pramen-py

Cli application for defining the data transformations for Pramen.

See:
```bash
pramen-py --help
```
for more information.


## Installation

### App settings

Application configuration solved by the environment variables
(see .env.example)

### Add pramen-py as a dependency to your project

In case of poetry:

```bash
# ensure we have valid poetry environment
ls pyproject.toml || poetry init

poetry add pramen-py
```
In case of pip:

```bash
pip install pramen-py
```


## Usage

## Application configuration

In order to configure the pramen-py options you need to set
corresponding environment variables. To see the list of available options run:

```bash
pramen-py list-configuration-options
```

### Developing transformations

pramen-py uses python's
[namespace packages](https://packaging.python.org/en/latest/guides/packaging-namespace-packages/#native-namespace-packages)
for discovery of the transformations.

This mean, that in order to build a new transformer, it should be located
inside a python package with the `transformations` directory inside.

This directory should be declared as a package:
- for poetry
```toml
[tool.poetry]
# ...
packages = [
    { include = "transformations" },
]

```
- for setup.py
```python
from setuptools import setup, find_namespace_packages

setup(
    name='mynamespace-subpackage-a',
    # ...
    packages=find_namespace_packages(include=['transformations.*'])
)
```

Example files structure:
```
❯ tree .
.
├── README.md
├── poetry.lock
├── pyproject.toml
├── tests
│  └── test_identity_transformer.py
└── transformations
    └── identity_transformer
        ├── __init__.py
        └── example_config.yaml
```

In order to make transformer picked up by the pramen-py the following
conditions should be satisfied:
- python package containing the transformers should be installed to the
same python environment as pramen-py
- python package should have defined namespace package `transformations`
- transformers should extend `pramen_py.Transformation` base class

Subclasses created by extending Transformation base class are registered as
a cli command (pramen-py transformations run TransformationSubclassName)
with default options. Check:

```bash
pramen-py transformations run ExampleTransformation1 --help
```

for more details.

You can add your own cli options to your transformations. See example at
[ExampleTransformation2](transformations/example_trasformation_two/some_transformation.py)

### pramen-py pytest plugin

pramen-py also provides pytest plugin with helpful
fixtures to test created transformers.

List of available fixtures:
```bash
#install pramen-py into the environment and activate it
pytest --fixtures
# check under --- fixtures defined from pramen_py.test_utils.fixtures ---
```

pramen-py pytest plugin also loads environment variables from .env
file if it is presented in the root of the repo.

### Running and configuring transformations

Transformations can be run with the following command:
```bash
pramen-py transformations run \
  ExampleTransformation1 \
  --config config.yml \
  --info-date 2022-04-01
```

`--config` is required option for any transformation. See
[config_example.yaml](tests/resources/real_config.yaml) for more information.

To check available options and documentation for a particular transformation,
run:
```bash
pramen-py transformations run TransformationClassName --help
```
where TransformationClassName is the name of the transformation.

## Using as a Library
Read metastore tables by Pramen-Py API
```python
import datetime
from pyspark.sql import SparkSession
from pramen_py import MetastoreReader
from pramen_py.utils.file_system import FileSystemUtils

spark = SparkSession.getOrCreate()

hocon_config = FileSystemUtils(spark) \
    .load_hocon_config_from_hadoop("uri_or_path_to_file")

metastore = MetastoreReader(spark) \
    .from_config(hocon_config)

df_txn = metastore.get_table(
    "transactions",
    info_date_from=datetime.date(2022, 1, 1),
    info_date_to=datetime.date(2022, 6, 1)
)

df_customer = metastore.get_latest("customer")

df_txn.show(truncate=False)
df_customer.show(truncate=False)
```

## Development

Prerequisites:
- <https://python-poetry.org/docs/#installation>
- python 3.6

Setup steps:

```bash
git clone https://github.com/AbsaOSS/pramen
cd pramen-py
make install  # create virtualenv and install dependencies
make test
make pre-commit

# enable completions
# source <(pramen-py completions zsh)
# source <(pramen-py completions bash)

pramen-py --help
```


### Load environment configuration

Before doing any development step, you have to set your development
environment variables

```bash
make install
```

## Completions

```bash
# enable completions
source <(pramen-py completions zsh)
# or for bash
# source <(pramen-py completions bash)
```


## Deployment

### From the local development environment

```bash
# bump the version
vim pyproject.toml

# deploy to the dev environment (included steps of building and publishing
#   artefacts)
cat .env.ci
make publish
```
