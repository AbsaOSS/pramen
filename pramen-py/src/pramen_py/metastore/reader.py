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
import os.path

from typing import Optional

import attrs
import pyspark.sql.functions as F
import pyspark.sql.types as T

from loguru import logger
from pyspark.sql import DataFrame

from pramen_py.metastore.reader_base import MetastoreReaderBase
from pramen_py.models import TableFormat
from pramen_py.models.utils import get_metastore_table
from pramen_py.utils import convert_date_to_str, convert_str_to_date


@attrs.define(auto_attribs=True, slots=True)
class MetastoreReader(MetastoreReaderBase):
    """Interact with the data inside the file system in a convenient way.

    Provides interfaces for checking the data availability and
    retrieving these data.

    All methods which requires a table_name: str as input relies on
    the configuration. If the table_name will be missing in the config
    a KeyError will be raised.
    """

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

        info_date_from = info_date_from or self.info_date
        info_date_to = info_date_to or self.info_date

        table = get_metastore_table(table_name, self.config)

        logger.info(f"Looking for {table_name} in the metastore.")
        logger.debug(f"info_date range: {info_date_from} - {info_date_to}")

        if table.format is TableFormat.parquet:
            df = (
                self.spark.read.parquet(table.path)
                .filter(
                    F.col(table.info_date_settings.column) >= info_date_from,
                )
                .filter(
                    F.col(table.info_date_settings.column) <= info_date_to,
                )
            )
        # TODO #38 support delta format
        elif table.format is TableFormat.delta:
            raise NotImplementedError
        else:
            raise NotImplementedError
        logger.info(
            f"Table {table_name} successfully loaded from {table.path}."
        )
        if uppercase_columns:
            return df.select([F.col(c).alias(c.upper()) for c in df.columns])
        else:
            return df

    def get_latest(
        self,
        table_name: str,
        until: Optional[datetime.date] = None,
        uppercase_columns: bool = False,
    ) -> DataFrame:
        """Provides the latest partition of the table.

        if until is provided, then the partition will have until as
        upper boundary, otherwise the upper boundary will be the info_date
        """

        until = until or self.info_date
        logger.info(f"Getting latest partition for {table_name} until {until}")

        table = get_metastore_table(table_name, self.config)
        latest_date = convert_date_to_str(
            self.get_latest_available_date(table_name, until),
            fmt=table.info_date_settings.format,
        )
        path = os.path.join(
            table.path,
            f"{table.info_date_settings.column}={latest_date}",
        )
        logger.debug(f"Path to the latest partition is: {path}")
        if table.format is TableFormat.parquet:
            df = self.spark.read.parquet(path).withColumn(
                table.info_date_settings.column,
                F.lit(latest_date).cast(T.DateType()),
            )
        elif table.format is TableFormat.delta:
            raise NotImplementedError
        else:
            raise NotImplementedError
        logger.info(
            f"Table {table_name} with the latest partition {latest_date} "
            f"loaded"
        )
        if uppercase_columns:
            return df.select([F.col(c).alias(c.upper()) for c in df.columns])
        else:
            return df

    def get_latest_available_date(
        self,
        table_name: str,
        until: Optional[datetime.date] = None,
    ) -> datetime.date:
        """Provide the latest info_date available for the table.

        if until is not specified then info_date is used.
        """

        until = until or self.info_date
        logger.info(
            f"Getting latest available date for the table: {table_name} "
            f"until {until}"
        )
        table = get_metastore_table(table_name, self.config)

        def before_until(date: datetime.date) -> bool:
            return date <= until  # type: ignore

        def extract_dates(file_name: str) -> datetime.date:
            return convert_str_to_date(
                file_name.split("=")[1],
                fmt=table.info_date_settings.format,
            )

        files = self.fs_utils.list_files(
            table.path,
            file_pattern=f"{table.info_date_settings.column}=*",
        )
        logger.debug(
            f"The following files are in the {table.path}:\n"
            f"{chr(10).join(files)}"
        )

        try:
            latest_date = max(
                filter(
                    before_until,
                    map(
                        extract_dates,
                        files,
                    ),
                )
            )
        except ValueError as err:
            raise ValueError(
                f"No partitions are available for the given {table_name}. "
                f"The content of a table path is:\n"
                f"{chr(10).join(files)}\n"
                f"Only partitions earlier than {str(until)} might be included."
            ) from err
        else:
            logger.info(f"Latest date for {table_name} is {latest_date}")
            return latest_date

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

        from_date = from_date or self.info_date
        until_date = until_date or self.info_date

        logger.info(
            f"Checking for available data for {table_name} in period "
            f"between {from_date} to {until_date}"
        )

        try:
            latest_date = self.get_latest_available_date(
                table_name,
                until=until_date,
            )
        except ValueError:
            return False
        else:
            logger.debug(
                f"Data is available, checking if they are within necessary "
                f"date range: {from_date} - {until_date}"
            )
            return from_date <= latest_date <= until_date
