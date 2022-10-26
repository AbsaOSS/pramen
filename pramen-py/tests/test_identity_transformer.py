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

# TODO #30 add possibility to write more lightweight unit tests
#   - remove a need to construct config
#   - return df directly instead of writing it to the local fs
import datetime

import pytest

from chispa import assert_column_equality
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DateType

from pramen_py import MetastoreReader
from pramen_py.models import (
    InfoDateSettings,
    MetastoreTable,
    RunTransformer,
    TableFormat,
    TransformationConfig,
)
from transformations.identity_transformer import IdentityTransformer


@pytest.mark.asyncio
async def test_identity_transformer(
    spark: SparkSession,
    tmp_path_builder,
    when,
    transformer_runner,
    generate_df,
):
    # stub for transformer dependency table
    table_in_stub = generate_df(
        """
        +---+---+----------+
        |A  |B  |info_date |
        +---+---+----------+
        |1  |2  |2022-03-23|
        |3  |4  |2022-03-23|
        |5  |6  |2022-03-24|
        |7  |8  |2022-03-24|
        |9  |10 |2022-03-25|
        |11 |12 |2022-03-25|
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

    # random path which is used for transformer output table
    table_out_path = tmp_path_builder()
    config = TransformationConfig(
        run_transformers=[
            RunTransformer(
                name="IdentityTransformer",
                info_date=None,
                output_table="table_out",
                options={"table": "table_in"},
            ),
        ],
        metastore_tables=[
            MetastoreTable(
                name="table_in",
                format=TableFormat.parquet,
                path=tmp_path_builder().as_posix(),
                info_date_settings=InfoDateSettings(
                    column="information_date",
                    format="yyyy-MM-ddd",
                ),
            ),
            MetastoreTable(
                name="table_out",
                format=TableFormat.parquet,
                path=table_out_path.as_posix(),
                info_date_settings=InfoDateSettings(
                    column="information_date",
                    format="yyyy-MM-ddd",
                ),
            ),
        ],
    )

    # mocking metastore.get_table access to "table_in"
    (
        when(MetastoreReader, "get_table")
        .called_with("table_in")
        .then_return(table_in_stub)
    )

    (
        when(MetastoreReader, "get_latest")
        .called_with(
            "table_in",
            until=datetime.date(2022, 4, 1),
        )
        .then_return(table_in_stub)
    )

    # running the transformation
    await transformer_runner(
        IdentityTransformer,
        config,
        "2022-04-07",
    )

    # reading and preparing for the test output of the transformer
    actual = (
        spark.read.parquet(table_out_path.as_posix())
        .withColumn(
            "information_date_expected",
            lit("2022-04-07").cast(DateType()),
        )
        .withColumn(
            "transform_id_is_rand",
            (col("transform_id") >= 0) & (col("transform_id") <= 1),
        )
        .withColumn("transform_id_is_rand_expected", lit(True))
    )

    # perform needed checks
    assert_column_equality(
        actual,
        "information_date",
        "information_date_expected",
    )
    assert_column_equality(
        actual,
        "transform_id_is_rand",
        "transform_id_is_rand_expected",
    )


@pytest.mark.asyncio
async def test_identity_transformer_wrong_options(
    spark: SparkSession,
    tmp_path_builder,
    when,
    transformer_runner,
):
    config = TransformationConfig(
        run_transformers=[
            RunTransformer(
                name="IdentityTransformer",
                info_date=None,
                output_table="table_out",
                options={},
            ),
        ],
        metastore_tables=[
            MetastoreTable(
                name="table_in",
                format=TableFormat.parquet,
                path=tmp_path_builder().as_posix(),
                info_date_settings=InfoDateSettings(
                    column="information_date",
                    format="yyyy-MM-ddd",
                ),
            ),
            MetastoreTable(
                name="table_out",
                format=TableFormat.parquet,
                path=tmp_path_builder().as_posix(),
                info_date_settings=InfoDateSettings(
                    column="information_date",
                    format="yyyy-MM-ddd",
                ),
            ),
        ],
    )

    with pytest.raises(KeyError, match="Expected 'table' key"):
        await transformer_runner(
            IdentityTransformer,
            config,
            "2022-04-07",
        )
