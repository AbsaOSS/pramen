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

from typing import List, Optional

import attrs

from pyspark.sql import DataFrame, SparkSession

from pramen_py.models import MetastoreTable
from pramen_py.utils.file_system import FileSystemUtils


@attrs.define(auto_attribs=True, slots=True)
class MetastoreReaderBase(metaclass=abc.ABCMeta):
    """Interact with the data inside the file system in a convenient way.

    All implementations of the MetastoreReader should be inherited
    from this base class

    Provides interfaces for checking the data availability and
    retrieving these data.

    All methods which requires a table_name: str as input relies on
    the configuration. If the table_name will be missing in the config
    a KeyError should be raised.
    """

    spark: SparkSession = attrs.field()
    config: List[MetastoreTable] = attrs.field()
    info_date: datetime.date = attrs.field()
    fs_utils: FileSystemUtils = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        self.fs_utils = FileSystemUtils(spark=self.spark)

    @config.validator
    def check_config(
        self,
        _: attrs.Attribute,  # type: ignore
        value: List[MetastoreTable],
    ) -> None:
        for table in value:
            assert isinstance(table, MetastoreTable)

    @abc.abstractmethod
    def get_table(
        self,
        table_name: str,
        info_date_from: Optional[datetime.date] = None,
        info_date_to: Optional[datetime.date] = None,
        uppercase_columns: bool = False,
    ) -> DataFrame:
        """Get the table based on its name and config attributes.

        :param info_date_from: optional param with info_date as default
        :param info_date_to: optional param with info_date as default
        :param uppercase_columns: returns a table with uppercase column names

        if info_date_* params are provided, the data will be filtered
            based on it
        The data format (and other options) are obtained from the config.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def get_latest(
        self,
        table_name: str,
        until: Optional[datetime.date] = None,
        uppercase_columns: bool = False,
    ) -> DataFrame:
        """Provides the latest partition of the table.

        if until is provided, then the partition will have until as
        upper boundary, otherwise the upper boundary will be the info_date

        :param uppercase_columns: if True then all columns will be uppercase
        """

        raise NotImplementedError

    @abc.abstractmethod
    def get_latest_available_date(
        self,
        table_name: str,
        until: Optional[datetime.date] = None,
    ) -> datetime.date:
        """Provide the latest info_date available for the table.

        if until is not specified then info_date is used.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def is_data_available(
        self,
        table_name: str,
        from_date: Optional[datetime.date] = None,
        until_date: Optional[datetime.date] = None,
    ) -> bool:
        """Check if data is available for the given table.

        :param from_date: optional lower boundary. It is equal to the
            info_date by default
        :param until_date: optional upper boundary. It is equal to the
            info_date by default
        """

        raise NotImplementedError
