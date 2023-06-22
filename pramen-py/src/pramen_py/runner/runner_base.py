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

import abc

from typing import Callable, Optional, Type, TypeVar

import attrs
import click

from pyspark.sql import SparkSession
from typing_extensions import ParamSpec

from pramen_py.app import env
from pramen_py.models import RunTransformer, TransformationConfig
from pramen_py.transformation.transformation_base import (
    T_EXTRA_OPTIONS,
    Transformation,
)
from pramen_py.utils import get_or_create_spark_session


T_TRANSFORMATION = TypeVar("T_TRANSFORMATION", bound=Transformation)
CLI_CALLBACK = ParamSpec("CLI_CALLBACK")


def get_current_transformer_cfg(
    T: Type[T_TRANSFORMATION],
    config: TransformationConfig,
) -> RunTransformer:
    """Find corresponding transformer in the config.

    In case if the current transformer is not defined - raise an error.
    """
    try:
        transformer_to_run = next(
            filter(
                lambda t: t.name == T.__name__,
                config.run_transformers,
            )
        )
    except StopIteration:
        raise ValueError(
            f"{T.__name__} transformer missed "
            f"in the configuration. Currently the following transformers "
            f"are available in the configuration:\n"
            f"{chr(10).join(map(str,config.run_transformers))}"
        )
    else:
        return transformer_to_run


@attrs.define(auto_attribs=True, slots=True)
class Runner(metaclass=abc.ABCMeta):
    """Base Runner interface to manage Transformation run.

    Runner's implementations provides cli to control Transformations.
    Runners should be registered in pramen_py.app.cli.RunGroup.__init__
    after super().__init__(*args, **kwargs).

    There could be multiple runners registered in order to provide
    more control over the Transformations. For example the
    runner which executes all transformations asynchronously.
    """

    cli: click.Group = attrs.field()
    spark: SparkSession = attrs.field(default=None)

    @abc.abstractmethod
    def activate(self) -> None:
        """Runner entrypoint.

        Register cli interface, prepare resources etc.
        """
        ...

    @abc.abstractmethod
    def create_cli_cmd_for_transformations(self) -> None:
        """Setup cli command and interface for all available transformations.

        Use discover_transformations function to find all Transformation's
        subclasses.
        """
        ...

    @abc.abstractmethod
    def create_cli_cmd_callback(
        self,
        T: Type[T_TRANSFORMATION],
    ) -> Callable[CLI_CALLBACK, None]:
        """Define a transformation run callback.

        This is a command to be executed on a cli invocation.

        Since Transformation.run are defined as async function, use
        coro util function to prepare the event loop and schedule running
        the target using it. For example:

        >>> def create_cli_cmd_callback(
        >>>     self,
        >>>     T: Type[T_TRANSFORMATION],
        >>> ) -> Callable[CLI_CALLBACK, None]:
        >>>     async def t_run_wrapper(
        >>>         ctx: click.Context,
        >>>         config: TransformationConfig,
        >>>         info_date: Optional[str],
        >>>         **kwargs: T_EXTRA_OPTIONS,
        >>>     ) -> None:
        >>>         number_of_trials = env.int(
        >>>             "PRAMENPY_MAX_RETRIES_EXECUTE_TRANSFORMATION",
        >>>             1,
        >>>         )
        >>>         with logger.contextualize(
        >>>             transformer=f"{T.__name__}@{int(random() * 10000)}"
        >>>         ):
        >>>             await run_and_retry(
        >>>                 partial(
        >>>                     self.process_transformer,
        >>>                     T=T,
        >>>                     config=config,
        >>>                     info_date=info_date,
        >>>                     kwargs=kwargs,
        >>>                 ),
        >>>                 number_of_trials=number_of_trials,
        >>>                 exception=Py4JJavaError,
        >>>                 retry_hook=self.process_transformer_retry_hook,
        >>>             )

        >>>     return click.pass_context(coro(t_run_wrapper))
        """
        ...

    @abc.abstractmethod
    async def process_transformer(
        self,
        T: Type[T_TRANSFORMATION],
        config: TransformationConfig,
        info_date: Optional[str],
        **kwargs: T_EXTRA_OPTIONS,
    ) -> None:
        """Prepare and run the transformation.

        :param info_date: if specified should have precedence over the config
            value (use overwrite_info_dates utils function to update the
            config)

        IMPORTANT: the implementation should call the super class default
            implementation first, i.e.:

        >>> async def process_transformer(
        >>>     self,
        >>>     T: Type[T_TRANSFORMATION],
        >>>     config: TransformationConfig,
        >>>     info_date: Optional[str],
        >>>     **kwargs: T_EXTRA_OPTIONS,
        >>> ) -> None:
        >>>     await super().process_transformer(
        >>>         T,
        >>>         config,
        >>>         info_date,
        >>>         **kwargs,
        >>>     )

        building spark session at this stage makes sense, because
        this prepares the spark session (expensive) only in case of processing
        the tranformation.
        """

        self.spark = self.spark or get_or_create_spark_session(
            env,
            spark_config=get_current_transformer_cfg(T, config).spark_config,
        )
        ...

    @abc.abstractmethod
    async def process_transformer_retry_hook(self) -> None:
        """A hook to call on failure of the process_transformer.

        Should be used with run_and_retry utility function.
        """
        ...
