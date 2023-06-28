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
import pathlib

from typing import Dict, List, Optional

import attrs
import pyspark.sql.functions as F
import pyspark.sql.types as T

from loguru import logger
from pyhocon import ConfigTree  # type: ignore
from pyspark.sql import DataFrame, DataFrameReader
from pyspark.sql.utils import AnalysisException

from pramen_py.metastore.reader_base import MetastoreReaderBase
from pramen_py.models import InfoDateSettings, MetastoreTable, TableFormat
from pramen_py.models.utils import (
    get_metastore_table,
    get_table_format_by_value,
)
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

    HOCON_METASTORE_TABLES_VARIABLE = "pramen.metastore.tables"

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
        metastore_table = get_metastore_table(table_name, self.tables)
        logger.info(
            f"Getting latest partition '{metastore_table.info_date_settings.column}'"
            f" for the table: '{table_name}' until {until}"
        )

        latest_date = convert_date_to_str(
            self.get_latest_available_date(table_name, until),
            fmt=metastore_table.info_date_settings.format,
        )

        if metastore_table.format == TableFormat.parquet:
            path = os.path.join(
                metastore_table.path,
                f"{metastore_table.info_date_settings.column}={latest_date}",
            )
            df = (
                self._get_dataframe_reader(
                    metastore_table.format, metastore_table.reader_options
                )
                .load(pathlib.Path(path).as_posix())
                .withColumn(
                    metastore_table.info_date_settings.column,
                    F.lit(latest_date).cast(T.DateType()),
                )
            )
        else:
            df = self._read_delta_table(metastore_table).filter(
                F.col(metastore_table.info_date_settings.column)
                == F.lit(latest_date)
            )

        logger.info(
            f"Table '{table_name}' successfully loaded for {latest_date} from"
            f" {metastore_table.path or metastore_table.table}"
        )
        return self._apply_uppercase_to_columns_names(df, uppercase_columns)

    def get_table(
        self,
        table_name: str,
        info_date_from: Optional[datetime.date] = None,
        info_date_to: Optional[datetime.date] = None,
        uppercase_columns: bool = False,
    ) -> DataFrame:
        """Get the table based on its name and config attributes.

        :param table_name:
        :param info_date_from: optional param with info_date as default
        :param info_date_to: optional param with info_date as default
        :param uppercase_columns: returns a table with uppercase column names

        if info_date_* params are provided, the data will be filtered
            based on it
        The data format (and other options) are obtained from the config.
        """

        metastore_table = get_metastore_table(table_name, self.tables)

        info_date_from_str = convert_date_to_str(
            info_date_from or self.info_date,
            fmt=metastore_table.info_date_settings.format,
        )
        info_date_to_str = convert_date_to_str(
            info_date_to or self.info_date,
            fmt=metastore_table.info_date_settings.format,
        )

        logger.info(
            f"Looking for {table_name} in the metastore where\n"
            f"info_date in range: {info_date_from_str} - {info_date_to_str}."
        )

        if metastore_table.format == TableFormat.parquet:
            df = self._read_parquet_table(metastore_table)
        else:
            df = self._read_delta_table(metastore_table)

        df_filtered = df.filter(
            F.col(metastore_table.info_date_settings.column)
            >= F.lit(info_date_from_str),
        ).filter(
            F.col(metastore_table.info_date_settings.column)
            <= F.lit(info_date_to_str),
        )

        logger.info(
            f"Table {table_name} successfully loaded from {metastore_table.path}."
        )

        return self._apply_uppercase_to_columns_names(
            df_filtered, uppercase_columns
        )

    def get_latest_available_date(
        self,
        table_name: str,
        until: Optional[datetime.date] = None,
    ) -> datetime.date:
        """Provide the latest info_date available for the table.

        if until is not specified then info_date is used.
        """

        until = until or self.info_date
        metastore_table = get_metastore_table(table_name, self.tables)

        if metastore_table.format == TableFormat.parquet:
            file_paths = self.fs_utils.list_files(
                metastore_table.path,
                file_pattern=f"{metastore_table.info_date_settings.column}=*",
            )
            info_dates = self._extract_info_dates_from_partition_paths(
                file_paths, metastore_table
            )
        else:
            dataframe = self._read_delta_table(metastore_table)
            info_dates = self._extract_info_dates_from_dataframe(
                dataframe, metastore_table
            )

        filtered_info_dates = self._filter_info_dates_by_until(
            info_dates, until
        )

        if not filtered_info_dates:
            info_dates_as_string = list(
                map(
                    lambda date: convert_date_to_str(
                        date, metastore_table.info_date_settings.format
                    ),
                    filtered_info_dates,
                )
            )
            raise ValueError(
                f"No partitions are available for the given '{metastore_table.name}'.\n"
                f"The table is available for the following dates:\n"
                f"{info_dates_as_string}\n"
                f"Only partitions earlier than {str(until)} might be included"
            )

        return max(filtered_info_dates)

    def is_data_available(
        self,
        table_name: str,
        from_date: Optional[datetime.date] = None,
        until_date: Optional[datetime.date] = None,
    ) -> bool:
        """Check if data is available for the given table.

        :param table_name:
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

    def from_config(self, config: ConfigTree):  # type: ignore
        tables = config.get(self.HOCON_METASTORE_TABLES_VARIABLE)
        metastore_tables = []
        for table in tables:
            metastore = MetastoreTable(
                name=table.get("name", ""),
                format=get_table_format_by_value(table.get("format", "")),
                path=table.get("path", ""),
                table=table.get("table", ""),
                description=table.get("description", ""),
                records_per_partition=table.get(
                    "records_per_partition", 500000
                ),
                info_date_settings=InfoDateSettings(
                    column=table.get(
                        "information.date.column",
                        config.get(
                            "pramen.information.date.column",
                            "pramen_info_date",
                        ),
                    ),
                    format=table.get("information.date.format", "yyyy-MM-dd"),
                    start=table.get("information.date.start", None),
                ),
                reader_options=table.get("read.option", {}),
                writer_options=table.get("write.option", {}),
            )
            metastore_tables.append(metastore)
        self.tables = metastore_tables.copy()
        return self

    @staticmethod
    def _apply_uppercase_to_columns_names(
        df: DataFrame, uppercase_columns: bool
    ) -> DataFrame:
        if uppercase_columns:
            return df.select([F.col(c).alias(c.upper()) for c in df.columns])
        else:
            return df

    @staticmethod
    def _filter_info_dates_by_until(
        dates: List[datetime.date],
        until: datetime.date,
    ) -> List[datetime.date]:
        def before_until(date: datetime.date) -> bool:
            return date <= until

        return list(filter(before_until, dates))

    @staticmethod
    def _extract_date_from_path(
        path: str, target_partition_name: str, date_format: str
    ) -> Optional[datetime.date]:
        def is_partition_hidden(column_name: str, column_value: str) -> bool:
            return column_name.startswith(
                ("_", ".")
            ) or column_value.startswith(("_", "."))

        file_name = path.replace("\\", "/").rsplit("/", 1)[-1]
        if not file_name:
            return None

        name_parts = file_name.split("=")
        if not len(name_parts) == 2:
            return None

        partition_name, partition_value = name_parts
        if is_partition_hidden(partition_name, partition_value):
            return None

        if target_partition_name != partition_name:
            raise ValueError(
                f"Partition name mismatch for path: {path}\n"
                f"Expected '{target_partition_name}' instead '{partition_name}'"
            )

        try:
            return convert_str_to_date(partition_value, fmt=date_format)
        except (KeyError, ValueError):
            raise ValueError(
                f"Date format mismatch for path: {path}"
                f"Expected '{date_format}' instead '{partition_value}'"
            )

    def _extract_info_dates_from_partition_paths(
        self,
        file_paths: List[str],
        table: MetastoreTable,
    ) -> List[datetime.date]:
        raw_info_dates = [
            self._extract_date_from_path(
                path,
                table.info_date_settings.column,
                table.info_date_settings.format,
            )
            for path in file_paths
        ]

        filtered_info_dates: List[datetime.date] = [
            info_date for info_date in raw_info_dates if info_date is not None
        ]

        if not filtered_info_dates:
            raise ValueError(
                f"The directory does not contain partitions by "
                f"'{table.info_date_settings.column}': {table.path}"
            )

        return filtered_info_dates

    @staticmethod
    def _extract_info_dates_from_dataframe(
        df: DataFrame, table: MetastoreTable
    ) -> List[datetime.date]:
        info_date_settings = table.info_date_settings
        info_dates_df = df.select(info_date_settings.column).distinct()
        distinct_info_dates = [row[0] for row in info_dates_df.collect()]

        if not distinct_info_dates:
            raise ValueError(
                f"Metastore table '{table.name}' at '{table.path or table.table}'"
                f" does not contain any info dates (column: {info_date_settings.column})"
            )

        return distinct_info_dates

    def _get_dataframe_reader(
        self, table_format: TableFormat, reader_options: Dict[str, str]
    ) -> DataFrameReader:
        return self.spark.read.options(**reader_options).format(
            table_format.value
        )

    def _read_parquet_table(
        self, metastore_table: MetastoreTable
    ) -> DataFrame:
        try:
            return self._get_dataframe_reader(
                TableFormat.parquet, metastore_table.reader_options
            ).load(metastore_table.path)
        except AnalysisException:
            raise Exception(
                f"Unable to access directory: {metastore_table.path}"
            )

    def _read_delta_table(self, metastore_table: MetastoreTable) -> DataFrame:
        reader = self._get_dataframe_reader(
            TableFormat.delta, metastore_table.reader_options
        )

        if metastore_table.path:
            return reader.load(metastore_table.path)
        elif metastore_table.table:
            return reader.table(metastore_table.table)
        else:
            raise ValueError(
                f"Metastore table '{metastore_table.name}' needs to contain either path or table option"
            )
