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

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pramen_py import MetastoreReader, MetastoreWriter
from pramen_py.models import InfoDateSettings, MetastoreTable, TableFormat


@pytest.mark.parametrize(
    ("info_date", "expected", "exc", "exc_pattern"),
    (
        (
            d(2022, 3, 26),
            d(2022, 3, 26),
            None,
            None,
        ),
        (
            d(2022, 3, 24),
            d(2022, 3, 24),
            None,
            None,
        ),
        (
            d(2022, 3, 22),
            d(2022, 3, 24),
            ValueError,
            "No partitions are available",
        ),
    ),
)
def test_get_latest_available_date(
    spark,
    load_and_patch_config,
    info_date,
    expected,
    exc,
    exc_pattern,
):
    """Test get_latest_available_date.

    Our data has partitions between 2022-03-23 and 2022-04-26.
    """
    metastore = MetastoreReader(
        spark=spark,
        tables=load_and_patch_config.metastore_tables,
        info_date=info_date,
    )
    if exc:
        with pytest.raises(exc, match=exc_pattern):
            metastore.get_latest_available_date("table1_sync")
    else:
        assert metastore.get_latest_available_date("table1_sync") == expected


def test_for_delta_format(spark, test_dataframe, tmp_path):
    def save_delta_table(df: DataFrame, path: str) -> None:
        df.write.partitionBy("info_date").format("delta").mode(
            "overwrite"
        ).save(path)

    table_name = "latest_available_date"
    table_path = (
        tmp_path / "data_lake" / "example_test_tables" / table_name
    ).as_posix()

    df_union = test_dataframe.union(
        spark.createDataFrame(
            [(17, 18, d(2022, 11, 2))],
            test_dataframe.schema,
        )
    )
    save_delta_table(df_union, table_path)

    df_modify = df_union.withColumn(
        "info_date",
        F.when(
            df_union.info_date == F.lit("2022-11-02").cast(T.DateType()),
            F.lit("2022-11-01").cast(T.DateType()),
        ).otherwise(df_union.info_date),
    )
    save_delta_table(df_modify, table_path)

    metastore_table_config = MetastoreTable(
        name=table_name,
        format=TableFormat.delta,
        path=table_path,
        info_date_settings=InfoDateSettings(column="info_date"),
    )
    metastore = MetastoreReader(
        spark=spark,
        tables=[metastore_table_config],
    )

    assert metastore.get_latest_available_date(table_name) == d(2022, 11, 1)


@pytest.mark.parametrize(
    "table_format", (TableFormat.parquet, TableFormat.delta)
)
def test_returns_date_type(spark, tmp_path, test_dataframe, table_format):
    metastore_table = MetastoreTable(
        name="test_table",
        format=table_format,
        path=tmp_path.as_posix(),
        info_date_settings=InfoDateSettings(column="info_date"),
    )
    writer = MetastoreWriter(
        spark=spark,
        tables=[metastore_table],
        info_date=d(2023, 3, 23),
    )
    writer.write("test_table", test_dataframe)
    metastore_reader = MetastoreReader(spark=spark, tables=[metastore_table])

    latest_available_date = metastore_reader.get_latest_available_date(
        "test_table"
    )

    assert latest_available_date == d(2023, 3, 23)


@pytest.mark.parametrize(
    ("until", "info_date", "exp", "exc", "exc_msg"),
    (
        #
        # Check when until parameter is not limiting
        (
            None,
            d(2022, 3, 26),
            d(2022, 3, 26),
            None,
            None,
        ),
        (
            d(2022, 3, 26),
            d(2022, 3, 26),
            d(2022, 3, 26),
            None,
            None,
        ),
        (
            d(2022, 3, 30),
            d(2022, 3, 30),
            d(2022, 3, 26),
            None,
            None,
        ),
        #
        # Check when data has partitions newer than info_date
        (
            None,
            d(2022, 3, 23),
            d(2022, 3, 23),
            None,
            None,
        ),
        #
        # check when until filtering the output
        (
            d(2022, 3, 25),
            d(2022, 3, 26),
            d(2022, 3, 25),
            None,
            None,
        ),
        #
        # check when until filtering the output completely
        (
            d(2022, 3, 19),
            d(2022, 3, 26),
            None,
            ValueError,
            "No partitions are available",
        ),
    ),
)
def test_with_until(
    spark,
    load_and_patch_config,
    until,
    info_date,
    exp,
    exc,
    exc_msg,
):
    """Test get_latest.

    Our data has partitions between 2022-03-23 and 2022-04-26.
    We are testing the proper filtration logic based on until parameter
    as well as info_date.
    """
    metastore = MetastoreReader(
        spark=spark,
        tables=load_and_patch_config.metastore_tables,
        info_date=info_date,
    )
    if exc:
        with pytest.raises(exc, match=exc_msg):
            metastore.get_latest_available_date(
                "table1_sync",
                until=until,
            )
    else:
        actual = metastore.get_latest_available_date(
            "table1_sync",
            until=until,
        )
        assert actual == exp


def test_raises_value_error_on_bad_path(
    spark,
    config,
    monkeypatch,
):
    config.metastore_tables.append(
        MetastoreTable(
            name="bad_table",
            format=TableFormat.parquet,
            path="/i/am/not/exist",
            info_date_settings=InfoDateSettings(
                column="info_date",
                format="yyyy-MM-dd",
            ),
        ),
    )
    metastore = MetastoreReader(
        spark=spark,
        tables=config.metastore_tables,
        info_date=d(2022, 3, 26),
    )
    with pytest.raises(
        ValueError, match="The directory does not contain partitions"
    ):
        metastore.get_latest_available_date("bad_table")
