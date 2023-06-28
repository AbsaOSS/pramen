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

from pyspark.sql.types import DateType, StructField, StructType

from pramen_py import MetastoreReader
from pramen_py.models import InfoDateSettings, MetastoreTable, TableFormat


@pytest.fixture
def metastore_table():
    return MetastoreTable(
        name="test_table",
        format=TableFormat.parquet,
        path="/random/path",
        info_date_settings=InfoDateSettings(
            column="pramen_info_date", format="yyyy-MM-dd"
        ),
    )


@pytest.fixture
def metastore_reader(spark, metastore_table):
    return MetastoreReader(spark=spark, tables=[metastore_table])


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
def test_extract_date_from_path(
    metastore_reader, metastore_table, test_path, expected_date, exc, exc_msg
):
    if exc:
        with pytest.raises(exc, match=exc_msg):
            metastore_reader._extract_date_from_path(
                test_path,
                metastore_table.info_date_settings.column,
                metastore_table.info_date_settings.format,
            )
    else:
        date = metastore_reader._extract_date_from_path(
            test_path,
            metastore_table.info_date_settings.column,
            metastore_table.info_date_settings.format,
        )
        assert expected_date == date


def test_extract_from_multiple_paths(metastore_reader, metastore_table):
    file_paths = [
        "pramen-py/tests_table/.pramen_info_date=2022-04-01",
        "pramen-py/tests_table/pramen_info_date=2022-04-01",
        "pramen-py/tests_table/pramen_info_date=2022-04-02",
    ]

    info_dates = metastore_reader._extract_info_dates_from_partition_paths(
        file_paths, metastore_table
    )

    assert set(info_dates) == {d(2022, 4, 1), d(2022, 4, 2)}


def test_extract_from_multiple_paths_errors_on_empty_dir(
    metastore_reader, metastore_table
):
    file_paths = ["pramen-py/test_table/.pramen_info_date=2022-04-01"]

    with pytest.raises(
        ValueError,
        match="The directory does not contain partitions by 'pramen_info_date': /random/path",
    ):
        metastore_reader._extract_info_dates_from_partition_paths(
            file_paths, metastore_table
        )


def test_extract_info_dates_from_dataframe(
    spark, metastore_reader, metastore_table
):
    info_date_df = spark.createDataFrame(
        [(d(2022, 4, 1),), (d(2022, 4, 2),)],
        StructType(
            [
                StructField(
                    metastore_table.info_date_settings.column, DateType()
                )
            ]
        ),
    ).toDF(metastore_table.info_date_settings.column)

    info_dates = metastore_reader._extract_info_dates_from_dataframe(
        info_date_df, metastore_table
    )

    assert set(info_dates) == {d(2022, 4, 1), d(2022, 4, 2)}


def test_extract_info_dates_from_empty_dataframe_raises_error(
    spark, metastore_reader, metastore_table
):
    empty_info_date_df = spark.createDataFrame(
        [],
        StructType(
            [
                StructField(
                    metastore_table.info_date_settings.column, DateType()
                )
            ]
        ),
    )

    with pytest.raises(
        ValueError,
        match=r"Metastore table 'test_table' at '/random/path' "
        r"does not contain any info dates \(column: pramen_info_date\)",
    ):
        metastore_reader._extract_info_dates_from_dataframe(
            empty_info_date_df, metastore_table
        )
