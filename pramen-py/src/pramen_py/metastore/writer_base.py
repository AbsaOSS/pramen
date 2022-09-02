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
import datetime

from typing import List

import attrs

from pyspark.sql import DataFrame, SparkSession

from pramen_py.models import MetastoreTable


@attrs.define(auto_attribs=True, slots=True)
class MetastoreWriterBase(metaclass=abc.ABCMeta):
    """Adds writing capabilities to the Metastore.

    All implementations of the MetastoreWriter should be inherited
    from this base class

    Makes it possible to refer tables based on it name and configurations.
    """

    spark: SparkSession = attrs.field()
    config: List[MetastoreTable] = attrs.field()
    info_date: datetime.date = attrs.field()

    @config.validator
    def check_config(  # type: ignore
        self,
        _,
        value: List[MetastoreTable],
    ) -> None:
        for table in value:
            assert isinstance(table, MetastoreTable)

    @abc.abstractmethod
    def write(
        self,
        table_name: str,
        df: DataFrame,
    ) -> None:
        """Write the given table in accordance with the configuration."""

        raise NotImplementedError
