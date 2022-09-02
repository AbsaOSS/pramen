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

import asyncio
import importlib
import os
import pkgutil

from datetime import date, datetime
from functools import wraps
from pathlib import Path
from time import sleep
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Optional,
    Type,
    TypeVar,
    Union,
)

from environs import Env
from loguru import logger
from pyspark.sql import SparkSession


OPS_RET = TypeVar("OPS_RET")

DATE_FMTS = {
    "yyyy-MM-dd": "%Y-%m-%d",
    "yyyyMMdd": "%Y%m%d",
}
DEFAULT_DATE_FMT = "yyyy-MM-dd"
SPARK_DEFAULT_CONFIG = {
    "spark.master": "yarn",
    "spark.submit.deployMode": "client",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.maxExecutors": "32",
}


def convert_str_to_date(
    s: str,
    fmt: str = DEFAULT_DATE_FMT,
    date_fmts: Dict[str, str] = DATE_FMTS,
) -> date:
    try:
        return datetime.strptime(s, date_fmts[fmt]).date()
    except KeyError:
        raise KeyError(
            f"Unknown date format {fmt}."
            f"The list of known formats are: "
            f"{', '.join(DATE_FMTS)}"
        )
    except ValueError:
        raise ValueError(f"Can't convert {s} to the {fmt}")


def convert_date_to_str(
    d: date,
    fmt: str = DEFAULT_DATE_FMT,
) -> str:
    try:
        return d.strftime(DATE_FMTS[fmt])
    except KeyError:
        raise KeyError(
            f"Unknown date format {fmt}."
            f"The list of known formats are: "
            f"{', '.join(DATE_FMTS)}"
        )


def load_modules(d: Path) -> None:
    """Load python modules recursively."""

    def inner(package: str) -> None:
        package_mod = importlib.import_module(package)
        for _, name, is_pkg in pkgutil.walk_packages(package_mod.__path__):
            full_name = package_mod.__name__ + "." + name
            logger.debug(
                f"Importing module {full_name} from {package_mod.__path__}"
            )
            importlib.import_module(full_name)
            if is_pkg:
                inner(full_name)

    inner(d.as_posix())


def coro(
    f: Callable[
        ...,
        Coroutine[Any, Any, OPS_RET],
    ]
) -> Callable[..., OPS_RET]:
    """Wrap coroutine into synchronous function.

    Run it with the current event loop.

    click lib can't work with the coroutines.
    """

    @wraps(f)
    def wrapper(
        *args: object,
        **kwargs: object,
    ) -> OPS_RET:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))

    return wrapper


def get_or_create_spark_session(
    env: Env,
    spark_config: Optional[Dict[str, str]] = None,
    force_recreate: bool = False,
) -> SparkSession:
    """Create or return existing SparkSession.

    In case force_recreate=True, the current session will be
    destroyed and a new one will be created.

    spark_config takes precedence over config defined via
    environment variable PRAMENPY_SPARK_CONFIG
    """
    logger.info("Preparing SparkSession")
    os.environ.update(
        {
            "JAVA_HOME": env.str(
                "PRAMENPY_SPARK_JAVA_HOME",
                os.environ.get("JAVA_HOME", ""),
            )
        }
    )

    maybe_active_session = SparkSession.getActiveSession()
    if maybe_active_session and force_recreate:
        logger.info("Force recreating a spark session")
        spark = maybe_active_session
        logger.info("Stopping active session and waiting for 3s...")
        spark.sparkContext.stop()
        spark.stop()
        sleep(3)

    session = SparkSession.builder.config(
        "spark.app.name",
        "pramen-py",
    )
    selected_config = spark_config or env.dict(
        "PRAMENPY_SPARK_CONFIG",
        SPARK_DEFAULT_CONFIG,
    )
    for k, v in selected_config.items():
        logger.debug(f"Setting spark config option: {k} to {v}")
        session.config(k, v)

    spark = session.getOrCreate()
    logger.info("SparkSession is ready")
    return spark


T_FN_ARGS = TypeVar("T_FN_ARGS")
T_FN_KWARGS = TypeVar("T_FN_KWARGS")
T_FN_RETURN = TypeVar("T_FN_RETURN")


async def run_and_retry(
    fn: Union[
        Callable[[T_FN_ARGS, T_FN_KWARGS], T_FN_RETURN],
        Callable[[T_FN_ARGS, T_FN_KWARGS], Awaitable[T_FN_RETURN]],
    ],
    *,
    number_of_trials: int,
    exception: Type[BaseException],
    retry_hook: Callable[[], Awaitable[None]],
) -> T_FN_RETURN:
    """Run a function until success or fail after number_of_trials.

    fn: a callable (could be also async function). If it has args and kwargs,
        use functools.partial(fn, *args, **kwargs)
    number_of_trials: number of times the function will be retried if
        exception raised
    exception: exception which will be caught and processed by triggering
        teh retry_hook and rerunning the function
    retry_hook: is a coroutine function which is called if the fn function
        fails

    Example:
    >>> def some_sync_foo(a, b):
    >>>     return a + b

    >>> async def some_async_foo(a, b):
    >>>     return a + b

    >>> assert 3 == await run_and_retry(
    >>>     partial(some_sync_foo, 1, 2),
    >>>     number_of_trials=2,
    >>>     exception=ValueError,
    >>>     retry_hook=AsyncMock(),
    >>> )
    >>> assert 3 == await run_and_retry(
    >>>     partial(some_async_foo, 1, 2),
    >>>     number_of_trials=2,
    >>>     exception=ValueError,
    >>>     retry_hook=AsyncMock(),
    >>> )

    """

    async def inner(
        trial: int,
        exp: Optional[Type[BaseException]] = None,
    ) -> T_FN_RETURN:
        if trial <= 0:
            raise exp  # type: ignore
        try:
            logger.info(f"Trials last: {trial} from total {number_of_trials}")
            try:
                result = await fn()  # type: ignore
            except TypeError as err:
                if "can't be used in 'await' expression" in str(err):
                    result = fn()  # type: ignore
                else:
                    raise
        except exception as exp:
            logger.error("Trial failed")
            await retry_hook()
            return await inner(trial - 1, exp)  # type: ignore
        else:
            logger.info("Trial succeeded")
            return result  # type: ignore

    # in case of partial, fn is hidden under fn.func attrib
    logger.info(
        f"Running function {fn}. "
        f"It will be tried {number_of_trials} times until success."
    )
    with logger.contextualize(tracked_by_retry=True):
        assert (
            number_of_trials > 0
        ), f"can't try to run a function {number_of_trials} times."
        return await inner(number_of_trials)
