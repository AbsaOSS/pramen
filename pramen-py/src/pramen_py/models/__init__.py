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
import enum
import pprint

from typing import Dict, List, Optional

import attrs
import cattr
import yaml

from click import Context, Option
from loguru import logger

from pramen_py.app import env
from pramen_py.utils import DEFAULT_DATE_FMT, convert_str_to_date


cattr.register_structure_hook(datetime.date, lambda v, t: v)  # type: ignore
cattr.register_unstructure_hook(datetime.date, lambda v: str(v))  # type: ignore


@enum.unique
class TableFormat(enum.Enum):
    parquet: str = "parquet"
    delta: str = "delta"


@attrs.define(auto_attribs=True, frozen=True, slots=True)
class InfoDateSettings:
    column: str = attrs.field()
    format: str = attrs.field(default="yyyy-MM-dd")
    start: Optional[datetime.date] = attrs.field(default=None)

    def __attrs_post_init__(self) -> None:
        # converter to datetime based on format
        if self.start and isinstance(self.start, str):
            logger.debug(
                f"Received {InfoDateSettings} with string as a date. "
                f"Converting {self.start} to the date with the "
                f"following format {self.format}"
            )
            object.__setattr__(
                self,
                "start",
                convert_str_to_date(self.start, self.format),
            )


@attrs.define(auto_attribs=True, frozen=True, slots=True)
class MetastoreTable:
    name: str = attrs.field()
    format: TableFormat = attrs.field()
    path: str = attrs.field()
    info_date_settings: InfoDateSettings = attrs.field()
    description: str = attrs.field(default="")
    records_per_partition: int = attrs.field(default=500000)

    def __attrs_post_init__(self) -> None:
        default_fs = env.str("PRAMENPY_DEFAULT_FS", "hdfs").strip('"')
        if default_fs != "local" and self.path.startswith("/"):
            logger.debug(
                f"Filesystem is {default_fs}, but path is {self.path}. "
                f"Appending {default_fs}:// prefix."
            )
            object.__setattr__(
                self,
                "path",
                f"{default_fs}://{self.path}",
            )


@attrs.define(auto_attribs=True, frozen=True, slots=True)
class RunTransformer:
    name: str = attrs.field()
    info_date: datetime.date = attrs.field()
    output_table: str = attrs.field()
    options: Dict[str, str] = attrs.field(factory=dict)
    spark_config: Dict[str, str] = attrs.field(factory=dict)

    def __attrs_post_init__(self) -> None:
        # converter to datetime
        if self.info_date and isinstance(self.info_date, str):
            logger.debug(
                f"Received {RunTransformer} with string as a date. "
                f"Converting {self.info_date} to the date with the "
                f"default format: {DEFAULT_DATE_FMT}"
            )
            object.__setattr__(
                self,
                "info_date",
                convert_str_to_date(
                    self.info_date,
                    DEFAULT_DATE_FMT,
                ),
            )


@attrs.define(auto_attribs=True, frozen=True, slots=True)
class TransformationConfig:
    run_transformers: List[RunTransformer] = attrs.field()
    metastore_tables: List[MetastoreTable] = attrs.field()


def serialization_callback(
    _: Context,
    __: Option,
    value: str,
) -> TransformationConfig:
    logger.info(
        f"Serializing and type checking the configuration received from "
        f"{value}"
    )
    with open(value) as config_f:
        config: Dict[str, str] = yaml.load(config_f, Loader=yaml.BaseLoader)
    try:
        serialized_config = cattr.structure(config, TransformationConfig)
        logger.info(pprint.pformat(cattr.unstructure(serialized_config)))
        return serialized_config
    except Exception as err:
        raise TypeError(f"Config validation failed:\n{err}") from err
