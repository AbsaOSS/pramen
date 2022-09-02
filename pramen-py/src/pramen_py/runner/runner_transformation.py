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

from functools import partial
from random import random
from typing import Callable, Generator, Optional, Type, TypeVar, cast

import attrs
import click

from loguru import logger
from py4j.protocol import Py4JJavaError  # type: ignore

from pramen_py import MetastoreReader, MetastoreWriter
from pramen_py.app import env
from pramen_py.models import TransformationConfig, serialization_callback
from pramen_py.runner.runner_base import (
    CLI_CALLBACK,
    Runner,
    get_current_transformer_cfg,
)
from pramen_py.transformation.transformation_base import (
    T_EXTRA_OPTIONS,
    Transformation,
)
from pramen_py.utils import (
    convert_str_to_date,
    coro,
    get_or_create_spark_session,
    load_modules,
    run_and_retry,
)


T_TRANSFORMATION = TypeVar("T_TRANSFORMATION", bound=Transformation)

TRANSFORMATIONS_DIR = pathlib.Path("./transformations")


def overwrite_info_dates(
    config: TransformationConfig,
    info_date: str,
) -> TransformationConfig:
    """Force update the info_date in transformation configs."""
    info_date_ = convert_str_to_date(info_date)
    for transformer in config.run_transformers:
        if transformer.info_date != info_date_:
            logger.warning(
                f"info_date which is provided via cli overwrites the date "
                f"in the transformer {transformer.name}:\n"
                f"{transformer.info_date} -> {info_date_}"
            )
            object.__setattr__(transformer, "info_date", info_date_)
    return config


def discover_transformations(
    path: pathlib.Path = TRANSFORMATIONS_DIR,
) -> Generator[Type[T_TRANSFORMATION], None, None,]:
    """Return generator of Transformation subclasses.

    In order to provide a set of subclasses we keep track already
    provided class names and do not allow to yield a class with the same
    name more than one time.
    """
    load_modules(path)
    _transformations_names = []
    for cls in Transformation.__subclasses__():
        if cls.__name__ not in _transformations_names:
            _transformations_names.append(cls.__name__)
            yield cast(Type[T_TRANSFORMATION], cls)


@attrs.define(auto_attribs=True, slots=True)
class TransformationsRunner(Runner):
    """Impl of the Runner of the Transformation.

    This runner has the following functionalities:
    - Adds cli cmd for each transformation, with the predefined set of options
        (--config and --info-date). It is also allows to forward
        any other options which are provided via cli_options attribute of the
        Transformation base class
    - Retry running the transformer in case of a failure (see the
        pramen-py list-configuration-options)
    - Transformation command executed asynchronously with the dedicated event
        loop
    """

    def activate(self) -> None:
        self.create_cli_cmd_for_transformations()

    def create_cli_cmd_for_transformations(self) -> None:
        for T in discover_transformations():  # type: ignore
            self.cli.add_command(
                click.Command(
                    name=T.__name__,
                    callback=self.create_cli_cmd_callback(T),
                    params=[
                        click.Option(
                            ("-c", "--config"),
                            help="Transformation config file",
                            required=True,
                            callback=serialization_callback,
                            type=click.Path(exists=True),
                        ),
                        click.Option(
                            ("--info-date",),
                            help=(
                                "Information date. If specified, takes precedence "
                                "over the config values"
                            ),
                        ),
                        *T.cli_options,
                    ],
                    help=T.run.__doc__,
                )
            )

    def create_cli_cmd_callback(
        self,
        T: Type[T_TRANSFORMATION],
    ) -> Callable[CLI_CALLBACK, None]:
        async def t_run_wrapper(
            ctx: click.Context,
            config: TransformationConfig,
            info_date: Optional[str],
            **kwargs: T_EXTRA_OPTIONS,
        ) -> None:
            number_of_trials = env.int(
                "PRAMENPY_MAX_RETRIES_EXECUTE_TRANSFORMATION",
                1,
            )
            with logger.contextualize(
                transformer=f"{T.__name__}@{int(random() * 10000)}"
            ):
                await run_and_retry(
                    partial(  # type: ignore
                        self.process_transformer,
                        T=T,
                        config=config,
                        info_date=info_date,
                        kwargs=kwargs,
                    ),
                    number_of_trials=number_of_trials,
                    exception=Py4JJavaError,
                    retry_hook=self.process_transformer_retry_hook,
                )

        return cast(
            Callable[CLI_CALLBACK, None],
            click.pass_context(coro(t_run_wrapper)),
        )

    async def process_transformer(
        self,
        T: Type[T_TRANSFORMATION],
        config: TransformationConfig,
        info_date: Optional[str],
        **kwargs: T_EXTRA_OPTIONS,
    ) -> None:
        await super().process_transformer(
            T,
            config,
            info_date,
            **kwargs,
        )

        t = T(
            spark=self.spark,
            config=config,
        )

        logger.info("Executing transformer")
        t_cfg = get_current_transformer_cfg(T, config)
        if info_date:
            config = overwrite_info_dates(config, info_date)

        metastore_reader = MetastoreReader(
            spark=self.spark,
            config=config.metastore_tables,
            info_date=t_cfg.info_date,
        )
        metastore_writer = MetastoreWriter(
            spark=self.spark,
            config=config.metastore_tables,
            info_date=t_cfg.info_date,
        )

        transformer_result = await t.run(
            metastore_reader,
            t_cfg.info_date,
            t_cfg.options,
            **kwargs,
        )
        metastore_writer.write(
            t_cfg.output_table,
            df=transformer_result,
        )
        logger.info("Transformer executed successfully")

    async def process_transformer_retry_hook(self) -> None:
        self.spark = get_or_create_spark_session(
            env,
            force_recreate=True,
        )
