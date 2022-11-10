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

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import rand

from pramen_py import T_EXTRA_OPTIONS, MetastoreReader, Transformation


class IdentityTransformer(Transformation):
    async def run(
        self,
        metastore: MetastoreReader,
        info_date: datetime.date,
        options: Dict[str, str],
        **kwargs: T_EXTRA_OPTIONS,
    ) -> DataFrame:
        try:
            table_name = options["table"]
        except KeyError:
            raise KeyError(
                f"Expected 'table' key in options. Received options: {options}"
            )
        # just as example
        df = metastore.get_table(table_name)

        empty_allowed = options.get("empty.allowed", "true").lower() == "true"

        assert (
            empty_allowed or df.count() != 0
        ), f"Empty table {table_name} received"

        return df.withColumn("transform_id", rand())
