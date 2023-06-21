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
def test_is_data_available(
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
