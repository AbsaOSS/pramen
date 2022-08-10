import datetime

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import rand
from syncwatcher_py import T_EXTRA_OPTIONS, MetastoreReader, Transformation


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

        df = metastore.get_latest(
            table_name,
            until=datetime.date(2022, 4, 1),
        )
        assert df.count() != 0, f"Empty table {table_name} received"
        return df.withColumn("transform_id", rand())
