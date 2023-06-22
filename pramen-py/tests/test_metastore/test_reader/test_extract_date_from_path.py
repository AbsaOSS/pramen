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

from pramen_py import MetastoreReader
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
