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

import datetime
import pathlib

from typing import Tuple

import cattr
import pytest
import yaml

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, IntegerType, StructField, StructType

from pramen_py.models import TransformationConfig


REPO_ROOT = pathlib.Path(__file__).parents[1]


@pytest.fixture()
def repo_root() -> pathlib.Path:
    return REPO_ROOT


@pytest.fixture
def create_parquet_data_stubs(
    spark: SparkSession,
    tmp_path: pathlib.Path,
) -> Tuple[pathlib.Path, DataFrame]:
    """Create parquet data stubs partitioned by info_date.

    +---+---+----------+
    |  A|  B| info_date|
    +---+---+----------+
    |  1|  2|2022-03-23|
    |  3|  4|2022-03-23|
    |  5|  6|2022-03-24|
    |  7|  8|2022-03-24|
    |  9| 10|2022-03-25|
    | 11| 12|2022-03-25|
    | 13| 14|2022-03-26|
    | 15| 16|2022-03-26|
    +---+---+----------+

    The path is temporary and is removed after tests finishes.

    Returns tuple of the path with the parquet data and dataframe itself.
    """
    logger.info("Creating sample DataFrame partitioned by info_date")
    df = spark.createDataFrame(
        (
            (1, 2, datetime.date(2022, 3, 23)),
            (3, 4, datetime.date(2022, 3, 23)),
            (5, 6, datetime.date(2022, 3, 24)),
            (7, 8, datetime.date(2022, 3, 24)),
            (9, 10, datetime.date(2022, 3, 25)),
            (11, 12, datetime.date(2022, 3, 25)),
            (13, 14, datetime.date(2022, 3, 26)),
            (15, 16, datetime.date(2022, 3, 26)),
        ),
        StructType(
            [
                StructField("A", IntegerType()),
                StructField("B", IntegerType()),
                StructField("info_date", DateType()),
            ],
        ),
    )
    table_path = tmp_path / "data_lake" / "example_dependency_table"
    df.write.partitionBy("info_date").parquet(table_path.as_posix())
    logger.info("Dataframe successfully created")
    return table_path, df


@pytest.fixture
def config() -> TransformationConfig:
    """Build TransformationConfig from the resources/real_config.yaml."""
    with open(
        (REPO_ROOT / "tests/resources/real_config.yaml").as_posix()
    ) as config_f:
        config = yaml.load(config_f, Loader=yaml.BaseLoader)

    return cattr.structure(config, TransformationConfig)


@pytest.fixture
def load_and_patch_config(
    mocker,
    config,
    create_parquet_data_stubs,
):
    """Load config and patch tables pathes to use tmp_path from pytest.

    Config is loaded from resources/real_config.yaml

    Takes first table as existing table, where its path pointing to the
    create_parquet_data_stubs path. Last table is output table.

    For example to access output table of ExampleTransformation1:

    >>> # check if table was written indeed
    >>> output_table = spark.read.parquet(
    >>>     load_and_patch_config.metastore_tables[-1].path
    >>> )
    >>> assert output_table.count() == 8
    """
    object.__setattr__(
        config.metastore_tables[0],
        "path",
        create_parquet_data_stubs[0].resolve().as_posix(),
    )
    object.__setattr__(
        config.metastore_tables[-1],
        "path",
        (create_parquet_data_stubs[0].parent / "example_output_table")
        .resolve()
        .as_posix(),
    )

    def cattr_structure_config_side_effect(obj, cls):
        if cls is TransformationConfig:
            return config
        else:
            return cattr.structure(obj, cls)

    # we can't patch config_serialization_config, because click loads
    # it before pytest invokes this command, so we are patching cattrs
    mocker.patch.object(
        cattr,
        "structure",
        autospec=True,
        side_effect=cattr_structure_config_side_effect,
    )
    return config
