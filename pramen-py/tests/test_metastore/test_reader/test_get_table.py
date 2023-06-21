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
from chispa import assert_df_equality
from loguru import logger
from pyspark.sql import functions as F

from pramen_py import MetastoreReader, MetastoreWriter
from pramen_py.models import TableFormat, MetastoreTable, InfoDateSettings


@pytest.mark.parametrize(
    ("info_date", "info_date_from", "info_date_to", "exp_num_of_rows"),
    (
        #
        # check if the table has all rows when filters are not
        # limiting the data
        (d(2022, 3, 26), d(2022, 3, 23), d(2022, 3, 26), 8),
        (d(2022, 3, 26), d(2022, 3, 23), None, 8),
        #
        # check when filters should remove some data
        # (each partition date has 2 rows)
        (d(2022, 3, 26), None, None, 2),
        (d(2022, 3, 26), None, d(2022, 3, 26), 2),
        (d(2022, 3, 24), None, d(2022, 3, 25), 4),
        (d(2022, 3, 26), d(2022, 3, 24), d(2022, 3, 25), 4),
        (d(2022, 3, 26), d(2022, 3, 23), d(2022, 3, 23), 2),
        (d(2022, 3, 26), d(2022, 3, 22), d(2022, 3, 23), 2),
        (d(2022, 3, 26), d(2022, 3, 26), d(2022, 3, 26), 2),
        (d(2022, 3, 26), d(2022, 3, 26), d(2022, 3, 27), 2),
        #
        # check if the filter removes all the rows
        (d(2022, 3, 26), d(2022, 3, 21), d(2022, 3, 22), 0),
        (d(2022, 3, 22), None, None, 0),
    ),
)
def test_get_table(
    spark,
    generate_test_tables,
    info_date,
    info_date_from,
    info_date_to,
    exp_num_of_rows,
):
    """Test get_table.

    Our data has partitions between 2022-03-23 and 2022-04-26.
    """
    for format_ in TableFormat:
        metastore_table_config = MetastoreTable(
            name=f"read_table_{format_.value}",
            format=format_,
            path=generate_test_tables["read_table"][f"{format_.value}"],
            info_date_settings=InfoDateSettings(column="info_date"),
        )
        metastore = MetastoreReader(
            spark=spark,
            tables=[metastore_table_config],
            info_date=info_date,
        )
        table = metastore.get_table(
            f"read_table_{format_.value}",
            info_date_from=info_date_from,
            info_date_to=info_date_to,
        )
        assert table.count() == exp_num_of_rows, logger.error(
            f"Failed for format: {format_.value}"
        )


def test_with_reader_options(
    spark,
    get_data_stub,
    tmp_path,
):
    def get_metastore_writer(
        table: MetastoreTable, info_date: d
    ) -> MetastoreWriter:
        return MetastoreWriter(
            spark=spark,
            tables=[table],
            info_date=info_date,
        )

    reader_options = {"mergeSchema": "true"}
    metastore_table = MetastoreTable(
        name="test_table",
        format=TableFormat.parquet,
        path=tmp_path.as_posix(),
        info_date_settings=InfoDateSettings(column="info_date"),
        reader_options=reader_options,
    )
    get_metastore_writer(metastore_table, d(2022, 3, 23)).write(
        "test_table", get_data_stub.withColumn("C", F.lit(1))
    )
    get_metastore_writer(metastore_table, d(2022, 3, 24)).write(
        "test_table", get_data_stub.withColumn("D", F.lit("1"))
    )

    metastore_reader = MetastoreReader(spark=spark, tables=[metastore_table])
    dataframe = metastore_reader.get_table(
        "test_table",
        info_date_from=d(2022, 3, 23),
        info_date_to=d(2022, 3, 24),
    )

    assert set(dataframe.columns) == {"A", "B", "C", "D", "info_date"}


def test_columns_get_converted_to_uppercase(
    spark,
    generate_df,
    load_and_patch_config,
):
    metastore = MetastoreReader(
        spark=spark,
        tables=load_and_patch_config.metastore_tables,
        info_date=d(2022, 3, 26),
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
    actual = metastore.get_table(
        load_and_patch_config.metastore_tables[0].name
    )
    assert_df_equality(
        actual,
        expected,
        ignore_row_order=True,
        ignore_column_order=True,
    )

    actual = metastore.get_table(
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