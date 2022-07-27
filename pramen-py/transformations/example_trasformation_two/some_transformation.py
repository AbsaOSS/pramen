import datetime

from typing import Dict

import click

from loguru import logger
from pyspark.sql import DataFrame

from pramen_py import T_EXTRA_OPTIONS, MetastoreReader, Transformation


class ExampleTransformation2(Transformation):
    cli_options = [
        click.Option(
            ("-p", "--parse"),
            required=True,
        ),
    ]

    async def run(
        self,
        metastore: MetastoreReader,
        info_date: datetime.date,
        options: Dict[str, str],
        **kwargs: T_EXTRA_OPTIONS,
    ) -> DataFrame:
        """Example transformation 2."""
        logger.info("Hi from ExampleTransformation2!")
        dep_table = metastore.get_table("table1_sync")
        return dep_table
