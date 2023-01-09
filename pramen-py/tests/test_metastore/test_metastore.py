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
from pathlib import PurePath

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest

from chispa.dataframe_comparer import (
    DataFramesNotEqualError,
    assert_df_equality,
)
from chispa.schema_comparer import SchemasNotEqualError
from loguru import logger
from pyhocon import ConfigFactory  # type: ignore
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from pramen_py import MetastoreReader, MetastoreWriter
from pramen_py.models import InfoDateSettings, MetastoreTable, TableFormat


@pytest.mark.parametrize(
    ("test_path", "expected_date", "exc", "exc_msg"),
    (
        ("", None, None, None),
        (".pramen_info_date=__HIVE__", None, None, None),
        (
            "pramen-py/tests_table/.pramen_info_date=2022-04-01",
            None,
            None,
            None,
        ),
        (
            "pramen-py/tests_table/_pramen_info_date=2022-04-02",
            None,
            None,
            None,
        ),
        (
            "pramen-py/tests_table/pramen_info_date=_2022-04-03",
            None,
            None,
            None,
        ),
        (
            "pramen-py/tests_table/pramen_info_date=2022-04-04",
            d(2022, 4, 4),
            None,
            None,
        ),
        (
            "pramen-py\\tests_table\\pramen_info_date=2022-04-05",
            d(2022, 4, 5),
            None,
            None,
        ),
        (
            "pramen-py/tests_table\\pramen_info_date=2022-04-06",
            d(2022, 4, 6),
            None,
            None,
        ),
        (
            "pramen-py/tests_table/test_metastore/pramen_date=2022-04-07",
            None,
            ValueError,
            "Partition name mismatch for path",
        ),
        (
            "pramen-py/tests_table/test_metastore/pramen_info_date=08-04-2022",
            None,
            ValueError,
            "Date format mismatch for path",
        ),
        ("pramen_info_date=2022-04-09", d(2022, 4, 9), None, None),
        ("2022-04-10", None, None, None),
        ("=2022-04-11=", None, None, None),
        (
            "info_date=2022-04-12",
            None,
            ValueError,
            "Partition name mismatch for path",
        ),
        ("=2022-04-13", None, ValueError, "Partition name mismatch for path"),
        (
            "pramen_info_date=14-04-2022",
            None,
            ValueError,
            "Date format mismatch for path",
        ),
    ),
)
def test_extract_date_from_path(spark, test_path, expected_date, exc, exc_msg):
    metastore_table_config = MetastoreTable(
        name="test_table",
        format=TableFormat.parquet,
        path="User1/pramen-py/tests_table",
        info_date_settings=InfoDateSettings(
            column="pramen_info_date", format="yyyy-MM-dd"
        ),
    )
    metastore = MetastoreReader(
        spark=spark,
        tables=[metastore_table_config],
    )

    if exc:
        with pytest.raises(exc, match=exc_msg):
            metastore._extract_date_from_path(
                test_path,
                metastore_table_config.info_date_settings.column,
                metastore_table_config.info_date_settings.format,
            )
    else:
        date = metastore._extract_date_from_path(
            test_path,
            metastore_table_config.info_date_settings.column,
            metastore_table_config.info_date_settings.format,
        )
        assert expected_date == date


def test_metastore_read_table_error(spark, tmp_path):
    metastore_table_config = MetastoreTable(
        name="non_existing_table_name",
        format=TableFormat.parquet,
        path="non/existing/table/path",
        info_date_settings=InfoDateSettings(column="info_date"),
    )
    metastore = MetastoreReader(
        spark=spark,
        tables=[metastore_table_config],
    )

    with pytest.raises(Exception, match="Unable to access directory"):
        metastore._read_table(
            TableFormat.parquet,
            "non/existing/table/path",
            metastore_table_config.reader_options,
        )


def test_metastore_get_latest_available_date_for_delta(
    spark, get_data_stub, tmp_path
):
    def save_delta_table(df: DataFrame, path: str) -> None:
        df.write.partitionBy("info_date").format("delta").mode(
            "overwrite"
        ).save(path)

    table_name = "latest_available_date"
    table_path = (
        tmp_path / "data_lake" / "example_test_tables" / table_name
    ).as_posix()

    df_union = get_data_stub.union(
        spark.createDataFrame(
            [(17, 18, d(2022, 11, 2))],
            get_data_stub.schema,
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
def test_metastore_get_latest_available_date_with_until(
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


def test_metastore_raises_value_error_on_bad_path(
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
        tables=load_and_patch_config.metastore_tables,
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


def test_metastore_writer_write(spark: SparkSession, generate_test_tables):
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


def test_metastore_delta_writer_with_additional_options(
    spark, get_data_stub, tmp_path
):
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
            "test_table", get_data_stub.withColumn("C", F.lit(1))
        )
        metastore_writer.write(
            "test_table", get_data_stub.withColumn("D", F.lit("1"))
        )


def test_metastore_parquet_writer_with_additional_options(
    spark, get_data_stub, tmp_path
):
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

    metastore_writer.write("test_table", get_data_stub)

    snappy_parquets = pathlib.Path(metastore_table.path).rglob(
        "*.snappy.parquet"
    )
    gzipped_parquets = pathlib.Path(metastore_table.path).rglob("*.gz.parquet")
    assert len(list(snappy_parquets)) == 0
    assert len(list(gzipped_parquets)) > 0


def test_metastore_reader_with_additional_options(
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


def test_metastore_reader_get_table_uppercase(
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


def test_metastore_reader_get_latest_uppercase(
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


def test_metastore_reader_from_config(
    spark,
    repo_root,
) -> None:
    file_path = PurePath(
        repo_root / "tests/resources/test_metastore.conf"
    ).as_posix()
    hocon_config = ConfigFactory.parse_file(file_path)
    metastore = MetastoreReader(spark).from_config(hocon_config)

    assert len(metastore.tables) == 6
    assert metastore.tables[1].format == TableFormat.parquet
    assert metastore.tables[2].table == ""
    assert metastore.tables[5].table == "teller"
    assert metastore.tables[5].reader_options == {"mergeSchema": "false"}
    assert metastore.tables[5].writer_options == {"mergeSchema": "true"}
    assert metastore.tables[0] == MetastoreTable(
        name="lookup",
        format=TableFormat.delta,
        path="test4/lookup",
        table="",
        description="A lookup table",
        records_per_partition=10000,
        info_date_settings=InfoDateSettings(
            column="information_date",
            format="yyyy-MM-dd",
            start=d(2022, 1, 1),
        ),
    )
