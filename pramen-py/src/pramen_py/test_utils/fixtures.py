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

import pathlib
import uuid

from functools import partial
from typing import Awaitable, Callable, Generator, Optional, Type

import pytest

from _pytest.logging import LogCaptureFixture
from environs import Env
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

from pramen_py import Transformation
from pramen_py.app import setup_logger
from pramen_py.models import TransformationConfig
from pramen_py.runner.runner_transformation import (
    T_TRANSFORMATION,
    TransformationsRunner,
)
from pramen_py.test_utils.spark_utils import generate_df_from_show_repr
from pramen_py.utils import get_or_create_spark_session


env = Env(expand_vars=True)
env.read_env(
    path=(pathlib.Path(".") / ".env").as_posix(),
    override=True,
)

logger.remove()
setup_logger(env=env)


@pytest.fixture
def caplog(
    caplog: LogCaptureFixture,
) -> Generator[LogCaptureFixture, None, None,]:
    """Adopt caplog to work with loguru."""
    handler_id = logger.add(caplog.handler, format="{message}")
    yield caplog
    logger.remove(handler_id)


@pytest.fixture()
def mocker(mocker: MockerFixture) -> MockerFixture:
    """Mocker fixture.

    Do nothing here except exposing typing info explicitly. Otherwise, type of
    mocker was not inferred properly.
    """
    return mocker


@pytest.fixture()
def tmp_path_builder(tmp_path: pathlib.Path) -> Callable[[], pathlib.Path]:
    """Provides a factory of the random path.

    Based on pytest tmp_path, so at the end of the test session the directory
    """

    def inner() -> pathlib.Path:
        return tmp_path / str(uuid.uuid4())

    return inner


@pytest.fixture()
def spark() -> SparkSession:
    """Spark session built the same as in syncwathcer_py."""
    return get_or_create_spark_session(env)


@pytest.fixture()
def transformer_runner(
    spark: SparkSession,
    mocker: MockerFixture,
) -> Callable[
    [
        Type[Transformation],
        TransformationConfig,
        Optional[str],
    ],
    Awaitable[None],
]:
    """Runner of the transformations with the given config and info_date.

    The metastore file system sets to local (PRAMENPY_DEFAULT_FS=local)

    Usage example:
    >>> await transformer_runner(
    >>>     IdentityTransformer,
    >>>     config,
    >>>     "2022-04-07",   # in "yyyy-MM-dd" format
    >>> )

    >>> # table_out_path is path defined in the config for output table
    >>> result_df = spark.read.parquet(table_out_path.as_posix())
    """

    async def inner(
        transformer_cls: Type[T_TRANSFORMATION],
        config: TransformationConfig,
        info_date: Optional[str] = None,
    ) -> None:
        runner = TransformationsRunner(
            cli=mocker.Mock(),
            spark=spark,
        )
        await runner.process_transformer(
            T=transformer_cls,
            config=config,
            info_date=info_date,
        )

    return inner


@pytest.fixture()
def generate_df(spark: SparkSession) -> Callable[[str, str], DataFrame]:
    """Generate spark df from df.show and df.printSchema strings."""
    return partial(generate_df_from_show_repr, spark)
