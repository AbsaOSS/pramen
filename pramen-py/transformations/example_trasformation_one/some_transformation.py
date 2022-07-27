import datetime

from typing import Dict

import attrs

from loguru import logger
from pyspark.sql import DataFrame

from pramen_py import T_EXTRA_OPTIONS, MetastoreReader, Transformation


@attrs.define(auto_attribs=True, slots=True)
class ExampleTransformation1(Transformation):
    async def run(
        self,
        metastore: MetastoreReader,
        info_date: datetime.date,
        options: Dict[str, str],
        **kwargs: T_EXTRA_OPTIONS,
    ) -> DataFrame:
        """Example transformation 1."""
        logger.info("Hi from ExampleTransformation1!")
        dep_table = metastore.get_table(
            "table1_sync",
            info_date_from=datetime.date(2022, 3, 23),
            info_date_to=datetime.date(2022, 3, 26),
        )
        return dep_table
