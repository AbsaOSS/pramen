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
import math
import os.path
import pathlib

from typing import Tuple

import attrs

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from pramen_py.metastore.writer_base import MetastoreWriterBase
from pramen_py.models import MetastoreTable, TableFormat
from pramen_py.models.utils import get_metastore_table


@attrs.define(auto_attribs=True, slots=True)
class MetastoreWriter(MetastoreWriterBase):
    """Adds writing capabilities to the Metastore.

    Makes it possible to refer tables based on it name and configurations.
    """

    def write(
        self,
        table_name: str,
        df: DataFrame,
    ) -> None:
        """Write the given table in accordance with the configuration."""

        logger.info(f"Writing table {table_name} started")

        target_table = get_metastore_table(table_name, self.config)
        if target_table.format == TableFormat.parquet:
            save_path, count_df_items = self._write_parquet_format_table(
                df, target_table
            )
        elif target_table.format == TableFormat.delta:
            save_path, count_df_items = self._write_delta_format_table(
                df, target_table
            )
        else:
            raise NotImplementedError
        logger.info(
            f"Successfully written {count_df_items} items to {table_name} at "
            f" {save_path}"
        )

    def _write_parquet_format_table(
        self, df: DataFrame, metastore_table: MetastoreTable
    ) -> Tuple[str, int]:
        target_path = os.path.join(
            metastore_table.path,
            f"{metastore_table.info_date_settings.column}={self.info_date}",
        )
        target_path = pathlib.Path(target_path).as_posix()
        df_dropped = df.drop(metastore_table.info_date_settings.column)
        df_repartitioned, count_df_items = self._apply_repartitioning(
            df_dropped, metastore_table.records_per_partition
        )
        df_repartitioned.write.format("parquet").mode("overwrite").save(
            target_path
        )
        return target_path, count_df_items

    def _write_delta_format_table(
        self, df: DataFrame, metastore_table: MetastoreTable
    ) -> Tuple[str, int]:
        df_with = df.withColumn(
            metastore_table.info_date_settings.column, lit(f"{self.info_date}")
        )
        df_repartitioned, count_df_items = self._apply_repartitioning(
            df_with, metastore_table.records_per_partition
        )
        df_writer = (
            df_repartitioned.write.format("delta")
            .mode("overwrite")
            .partitionBy(metastore_table.info_date_settings.column)
            .option("mergeSchema", "true")
            .option(
                "replaceWhere",
                f"{metastore_table.info_date_settings.column}='{self.info_date}'",
            )
        )
        if metastore_table.path:
            df_writer.save(metastore_table.path)
            return metastore_table.path, count_df_items
        elif metastore_table.table:
            df_writer.saveAsTable(metastore_table.table)
            return metastore_table.table, count_df_items
        else:
            raise NotImplementedError

    def _apply_repartitioning(
        self, df: DataFrame, records_per_partition: int
    ) -> Tuple[DataFrame, int]:
        count_df_items = df.count()
        if records_per_partition > 0:
            num_partitions = int(
                max(1, math.ceil(count_df_items / records_per_partition))
            )
            return df.repartition(num_partitions), count_df_items
        else:
            return df, count_df_items
