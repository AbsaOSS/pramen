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

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest

from chispa import assert_df_equality
from pyspark.sql import SparkSession

from pramen_py import MetastoreReader, MetastoreWriter
from pramen_py.models import InfoDateSettings, MetastoreTable, TableFormat


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
def test_metastore_get_latest_available_date(
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
        config=load_and_patch_config.metastore_tables,
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


def test_metastore_raises_valueerror_on_bad_path(
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
        config=config.metastore_tables,
        info_date=d(2022, 3, 26),
    )
    with pytest.raises(ValueError, match="No partitions"):
        metastore.get_latest_available_date("bad_table")


@pytest.mark.parametrize(
    (
        "table_name",
        "info_date",
        "from_date",
        "until_date",
        "exp",
        "exc",
        "exc_msg",
    ),
    (
        #
        # should return True if data is available
        (
            "test_table",
            d(2022, 3, 26),
            None,
            None,
            True,
            None,
            None,
        ),
        (
            "test_table",
            d(2022, 3, 26),
            None,
            d(2022, 3, 26),
            True,
            None,
            None,
        ),
        (
            "test_table",
            d(2022, 3, 26),
            d(2022, 3, 23),
            None,
            True,
            None,
            None,
        ),
        (
            "test_table",
            d(2022, 3, 26),
            d(2022, 3, 23),
            d(2022, 3, 26),
            True,
            None,
            None,
        ),
        (
            "test_table",
            d(2022, 3, 27),
            d(2022, 3, 22),
            d(2022, 3, 27),
            True,
            None,
            None,
        ),
        (
            "test_table",
            d(2022, 3, 26),
            d(2022, 3, 24),
            d(2022, 3, 25),
            True,
            None,
            None,
        ),
        (
            "test_table",
            d(2022, 3, 26),
            d(2022, 3, 24),
            d(2022, 3, 24),
            True,
            None,
            None,
        ),
        #
        # should return False when from until is out of range
        (
            "test_table",
            d(2022, 3, 26),
            None,
            d(2022, 3, 22),
            False,
            None,
            None,
        ),
        (
            "test_table",
            d(2022, 3, 26),
            d(2022, 3, 21),
            d(2022, 3, 22),
            False,
            None,
            None,
        ),
        (
            "test_table",
            d(2022, 3, 26),
            d(2022, 3, 27),
            d(2022, 3, 28),
            False,
            None,
            None,
        ),
        (
            "test_table",
            d(2022, 3, 26),
            d(2022, 3, 27),
            None,
            False,
            None,
            None,
        ),
        #
        # should return False when table has a bad name
        (
            "bad_name",
            d(2022, 3, 26),
            d(2022, 3, 24),
            d(2022, 3, 24),
            False,
            KeyError,
            "Table .+ missed in the config",
        ),
        # info_date is older than the most recent partition date should
        # give false
        (
            "test_table",
            d(2022, 3, 22),
            None,
            None,
            False,
            None,
            None,
        ),
    ),
)
def test_metastore_is_data_available(
    spark,
    load_and_patch_config,
    table_name,
    info_date,
    from_date,
    until_date,
    exp,
    exc,
    exc_msg,
):
    """Test is_data_available.

    Our data has partitions between 2022-03-23 and 2022-04-26.
    """
    metastore = MetastoreReader(
        spark=spark,
        config=load_and_patch_config.metastore_tables,
        info_date=info_date,
    )
    if exc:
        with pytest.raises(exc, match=exc_msg):
            metastore.is_data_available(
                table_name,
                from_date=from_date,
                until_date=until_date,
            )
    else:
        actual = metastore.is_data_available(
            "table1_sync",
            from_date=from_date,
            until_date=until_date,
        )
        assert actual is exp


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
def test_metastore_get_latest(
    spark: SparkSession,
    create_data_stubs_and_paths,
    info_date,
    until,
    exc,
    exc_msg,
):
    """Test get_latest.

    Our data has partitions between 2022-03-23 and 2022-04-26.
    """

    for format_ in TableFormat:
        path_to_table = create_data_stubs_and_paths[format_.value]
        metastore_table_config = MetastoreTable(
            name=f"table_{format_.value}",
            format=format_,
            path=path_to_table,
            info_date_settings=InfoDateSettings(column="info_date"),
        )
        metastore = MetastoreReader(
            spark=spark,
            config=[metastore_table_config],
            info_date=info_date,
        )
        if exc:
            with pytest.raises(exc, match=exc_msg):
                metastore.get_latest(
                    f"table_{format_.value}",
                    until=until,
                )
        else:
            actual = metastore.get_latest(
                f"table_{format_.value}",
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
            assert_df_equality(actual, expected, ignore_row_order=True)


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
def test_metastore_get_table(
    spark,
    create_data_stubs_and_paths,
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
            name=f"table_{format_.value}",
            format=format_,
            path=create_data_stubs_and_paths[format_.value],
            info_date_settings=InfoDateSettings(column="info_date"),
        )
        metastore = MetastoreReader(
            spark=spark,
            config=[metastore_table_config],
            info_date=info_date,
        )
        table = metastore.get_table(
            f"table_{format_.value}",
            info_date_from=info_date_from,
            info_date_to=info_date_to,
        )
        assert table.count() == exp_num_of_rows


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
    info_date,
    expected,
    exc,
    exc_pattern,
    load_and_patch_config,
):
    """Test get_latest_available_date.

    Our data has partitions between 2022-03-23 and 2022-04-26.
    """
    metastore = MetastoreReader(
        spark=spark,
        config=load_and_patch_config.metastore_tables,
        info_date=info_date,
    )
    if exc:
        with pytest.raises(exc, match=exc_pattern):
            metastore.get_latest_available_date("table1_sync")
    else:
        assert metastore.get_latest_available_date("table1_sync") == expected


def test_metastore_writer_write(
    spark: SparkSession, create_data_stubs_and_paths
):
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
            name=f"table_{format_.value}_output",
            format=format_,
            path=create_data_stubs_and_paths[f"output_{format_.value}"],
            info_date_settings=InfoDateSettings(column="INFORMATION_DATE"),
        )
        metastore = MetastoreWriter(
            spark=spark,
            config=[metastore_table_config],
            info_date=d(2022, 4, 6),
        )
        metastore.write(f"table_{format_.value}_output", df)

        actual = spark.read.parquet(
            create_data_stubs_and_paths[f"output_{format_.value}"]
        )
        expected = df.withColumn(
            "INFORMATION_DATE",
            F.lit("2022-04-06").cast(T.DateType()),
        )
        assert_df_equality(
            expected,
            actual,
            ignore_row_order=True,
        )


def test_metastore_reader_get_table_uppercase(
    spark,
    generate_df,
    load_and_patch_config,
):
    metastore = MetastoreReader(
        spark=spark,
        config=load_and_patch_config.metastore_tables,
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


def test_metastore_reader_get_latest_uppercase(
    spark,
    generate_df,
    load_and_patch_config,
):
    metastore = MetastoreReader(
        spark=spark,
        config=load_and_patch_config.metastore_tables,
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
