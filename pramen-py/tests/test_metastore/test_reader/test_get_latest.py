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

from datetime import date as d

import pytest

from chispa import DataFramesNotEqualError, assert_df_equality
from chispa.schema_comparer import SchemasNotEqualError
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pramen_py import MetastoreReader
from pramen_py.models import InfoDateSettings, MetastoreTable, TableFormat


@pytest.mark.parametrize(
    ("info_date", "until", "exc", "exc_msg"),
    (
        (
            d(2022, 3, 27),
            d(2022, 3, 27),
            None,
            None,
        ),
        (
            d(2022, 3, 27),
            None,
            None,
            None,
        ),
        (
            d(2022, 3, 26),
            d(2022, 3, 26),
            None,
            None,
        ),
        (
            d(2022, 3, 26),
            d(2022, 3, 25),
            None,
            None,
        ),
        (
            d(2022, 3, 22),
            None,
            ValueError,
            "No partitions are available",
        ),
        (
            d(2022, 3, 26),
            d(2022, 3, 19),
            ValueError,
            "No partitions are available",
        ),
    ),
)
def test_get_latest(
    spark: SparkSession,
    generate_test_tables,
    info_date,
    until,
    exc,
    exc_msg,
):
    """Test get_latest.

    Our data has partitions between 2022-03-23 and 2022-04-26.
    """

    for format_ in TableFormat:
        path_to_table = generate_test_tables["read_table"][f"{format_.value}"]
        metastore_table_config = MetastoreTable(
            name=f"read_table_{format_.value}",
            format=format_,
            path=path_to_table,
            info_date_settings=InfoDateSettings(column="info_date"),
        )
        metastore = MetastoreReader(
            spark=spark,
            tables=[metastore_table_config],
            info_date=info_date,
        )
        if exc:
            with pytest.raises(exc, match=exc_msg):
                metastore.get_latest(
                    f"read_table_{format_.value}",
                    until=until,
                )
        else:
            actual = metastore.get_latest(
                f"read_table_{format_.value}",
                until=until,
            )
            expected = spark.read.format(format_.value).load(path_to_table)
            latest_date = min(
                expected.select(F.col("info_date"))
                .orderBy(F.col("info_date"), ascending=False)
                .first()[0],
                until or info_date,
            )
            expected = expected.filter(F.col("info_date") == latest_date)
            try:
                assert_df_equality(actual, expected, ignore_row_order=True)
            except (DataFramesNotEqualError, SchemasNotEqualError):
                logger.error(f"Failed for format: {format_.value}")
                raise


def test_columns_get_converted_to_uppercase(
    spark,
    generate_df,
    load_and_patch_config,
):
    metastore = MetastoreReader(
        spark=spark,
        tables=load_and_patch_config.metastore_tables,
        info_date=d(2022, 8, 1),
    )

    expected = generate_df(
        """
        +---+---+----------+
        |A  |B  |info_date |
        +---+---+----------+
        |13 |14 |2022-03-26|
        |15 |16 |2022-03-26|
        +---+---+----------+
        """,
        """
        root
         |-- A: integer (nullable = true)
         |-- B: integer (nullable = true)
         |-- info_date: date (nullable = true)
        """,
    )
    actual = metastore.get_latest(
        load_and_patch_config.metastore_tables[0].name
    )
    assert_df_equality(
        actual,
        expected,
        ignore_row_order=True,
        ignore_column_order=True,
    )

    actual = metastore.get_latest(
        load_and_patch_config.metastore_tables[0].name,
        uppercase_columns=True,
    )
    expected = generate_df(
        """
        +---+---+----------+
        |A  |B  |INFO_DATE |
        +---+---+----------+
        |13 |14 |2022-03-26|
        |15 |16 |2022-03-26|
        +---+---+----------+
        """,
        """
        root
         |-- A: integer (nullable = true)
         |-- B: integer (nullable = true)
         |-- INFO_DATE: date (nullable = true)
        """,
    )
    assert_df_equality(
        actual,
        expected,
        ignore_row_order=True,
        ignore_column_order=True,
    )
