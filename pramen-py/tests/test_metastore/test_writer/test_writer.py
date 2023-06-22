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

import pathlib

from datetime import date as d

import pytest

from chispa import DataFramesNotEqualError, assert_df_equality
from chispa.schema_comparer import SchemasNotEqualError
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException

from pramen_py import MetastoreWriter
from pramen_py.models import InfoDateSettings, MetastoreTable, TableFormat


def test_write(spark: SparkSession, generate_test_tables):
    df = spark.createDataFrame(
        (
            (1, 2),
            (3, 4),
        ),
        T.StructType(
            [
                T.StructField("A", T.IntegerType()),
                T.StructField("B", T.IntegerType()),
            ],
        ),
    )

    for format_ in TableFormat:
        metastore_table_config = MetastoreTable(
            name=f"write_table_{format_.value}",
            format=format_,
            path=generate_test_tables["write_table"][f"{format_.value}"],
            info_date_settings=InfoDateSettings(column="INFORMATION_DATE"),
        )
        metastore = MetastoreWriter(
            spark=spark,
            tables=[metastore_table_config],
            info_date=d(2022, 4, 6),
        )
        metastore.write(f"write_table_{format_.value}", df)

        actual = spark.read.parquet(
            generate_test_tables["write_table"][f"{format_.value}"]
        )
        expected = df.withColumn(
            "INFORMATION_DATE",
            F.lit("2022-04-06").cast(T.DateType()),
        )
        try:
            assert_df_equality(actual, expected, ignore_row_order=True)
        except (DataFramesNotEqualError, SchemasNotEqualError):
            logger.error(f"Failed for format: {format_.value}")
            raise


def test_write_delta_with_options(spark, test_dataframe, tmp_path):
    writer_options = {"mergeSchema": "false"}
    metastore_table = MetastoreTable(
        name="test_table",
        format=TableFormat.delta,
        path=tmp_path.as_posix(),
        info_date_settings=InfoDateSettings(column="info_date"),
        writer_options=writer_options,
    )
    metastore_writer = MetastoreWriter(
        spark=spark,
        tables=[metastore_table],
        info_date=d(2022, 3, 24),
    )

    with pytest.raises(
        AnalysisException,
        match="A schema mismatch detected when writing to the Delta table",
    ):
        metastore_writer.write(
            "test_table", test_dataframe.withColumn("C", F.lit(1))
        )
        metastore_writer.write(
            "test_table", test_dataframe.withColumn("D", F.lit("1"))
        )


def test_write_parquet_with_options(spark, test_dataframe, tmp_path):
    """
    Testing additional parquet writer options for MetastoreTable.

    Since parquet writer does not have many meaningful options to specify, we test the 'compression' option.
    """
    writer_options = {"compression": "gzip"}
    table_path = tmp_path / "test_table"
    metastore_table = MetastoreTable(
        name="test_table",
        format=TableFormat.parquet,
        path=table_path.as_posix(),
        info_date_settings=InfoDateSettings(column="info_date"),
        writer_options=writer_options,
    )
    metastore_writer = MetastoreWriter(
        spark=spark,
        tables=[metastore_table],
        info_date=d(2022, 3, 24),
    )

    metastore_writer.write("test_table", test_dataframe)

    snappy_parquets = pathlib.Path(metastore_table.path).rglob(
        "*.snappy.parquet"
    )
    gzipped_parquets = pathlib.Path(metastore_table.path).rglob("*.gz.parquet")
    assert len(list(snappy_parquets)) == 0
    assert len(list(gzipped_parquets)) > 0
