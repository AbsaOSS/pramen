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

import os.path

import attrs

from loguru import logger
from pyspark.sql import DataFrame

from pramen_py.metastore.writer_base import MetastoreWriterBase
from pramen_py.models import TableFormat
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
        target_path = os.path.join(
            target_table.path,
            f"{target_table.info_date_settings.column}={self.info_date}",
        )
        df = df.drop(target_table.info_date_settings.column)
        df_writer = (
            # TODO #24 calculate and make repartition instead
            df.write.option(
                "maxRecordsPerFile",
                target_table.records_per_partition,
            )
        )
        if target_table.format is TableFormat.parquet:
            df_writer.parquet(target_path, mode="overwrite")
        elif target_table.format is TableFormat.delta:
            raise NotImplementedError
        else:
            raise NotImplementedError
        logger.info(
            f"Successfully written {df.count()} items to {table_name} at "
            f" {target_path}"
        )
