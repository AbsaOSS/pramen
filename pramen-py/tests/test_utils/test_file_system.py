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

import datetime

from pathlib import Path

import pytest

from pramen_py.models import InfoDateSettings, MetastoreTable
from pramen_py.utils.file_system import FileSystemUtils


@pytest.mark.parametrize(
    ("inputs", "outputs"),
    (
        (
            "file:///C:/Users/AB025F5/example_dependency_table",
            "file:///C:/Users/AB025F5/example_dependency_table",
        ),
        (
            "file://C:/Users/AB025F5/example_dependency_table",
            "file:///C:/Users/AB025F5/example_dependency_table",
        ),
        (
            "s3a://C:/Users/AB025F5/example_dependency_table",
            "s3a://C:/Users/AB025F5/example_dependency_table",
        ),
        (
            "hdfs://C:/Users/AB025F5/example_dependency_table",
            "hdfs://C:/Users/AB025F5/example_dependency_table",
        ),
        (
            "hdfs://Users/AB025F5/example_dependency_table",
            "hdfs://Users/AB025F5/example_dependency_table",
        ),
        (
            "//C:/Users/AB025F5/example_dependency_table",
            "file:///C:/Users/AB025F5/example_dependency_table",
        ),
        (
            "//Users/AB025F5/example_dependency_table",
            "file://Users/AB025F5/example_dependency_table",
        ),
        (
            "/Users/AB025F5/example_dependency_table",
            "file://Users/AB025F5/example_dependency_table",
        ),
        (
            "Users/AB025F5/example_dependency_table",
            "file://Users/AB025F5/example_dependency_table",
        ),
        (
            "file://Users/AB025F5/example_dependency_table",
            "file://Users/AB025F5/example_dependency_table",
        ),
        (
            "//D:/Users/AB025F5//example_dependency_table//",
            "file:///D:/Users/AB025F5/example_dependency_table",
        ),
        (
            "file:\\\\\\D:\\Users\\AB025F5\\example_dependency_table\\",
            "file:///D:/Users/AB025F5/example_dependency_table",
        ),
        (
            "file:\\D:\\Users\\AB025F5\\example_dependency_table\\",
            "file:///D:/Users/AB025F5/example_dependency_table",
        ),
        (
            "file:\\D:\\Users\\AB025F5\\example_dependency_table\\",
            "file:///D:/Users/AB025F5/example_dependency_table",
        ),
        (
            "\\D:\\Users\\AB025F5\\example_dependency_table\\",
            "file:///D:/Users/AB025F5/example_dependency_table",
        ),
        (
            "D:\\Users\\AB025F5\\example_dependency_table\\",
            "file:///D:/Users/AB025F5/example_dependency_table",
        ),
        (
            "D:\\Users\\AB025F5/example_dependency_table",
            "file:///D:/Users/AB025F5/example_dependency_table",
        ),
    ),
)
def test_ensure_proper_schema_for_local_fs(spark, inputs, outputs) -> None:
    test_fs_utils = FileSystemUtils(spark=spark)
    assert test_fs_utils.ensure_proper_schema_for_local_fs(inputs) == outputs


@pytest.mark.parametrize(
    ("expected"),
    (
        (
            "pramen {\n"
            '  environment.name = "TestEnv"\n'
            "  bookkeeping.enabled = true\n"
            "  bookkeeping.jdbc {\n"
            '    driver = "org.postgresql.Driver"\n'
            '    url = "jdbc:postgresql://oss-vip-1973.corp.dsarena.com:5433/syncwatcher_newpipeline"\n'
            '    user = "app_account"\n'
            "  }\n"
            "  warn.throughput.rps = 2000\n"
            "  good.throughput.rps = 40000\n"
            "}\n"
            "mail {\n"
            '  smtp.host = "smtp.absa.co.za"\n'
            '  send.from = "Pramen <pramen.noreply@absa.africa>"\n'
            '  send.to = "ruslan.iushchenko@absa.africa, Dennis.Chu@absa.africa"\n'
            "}\n"
            'sourcing.base.path = "/projects/lbamodels/datasync/batch/source"\n'
        ),
    ),
)
def test_read_file_from_hadoop(spark, repo_root, expected) -> None:
    file_path = Path(repo_root / "tests/resources/test_common.conf").as_posix()
    config_string = FileSystemUtils(spark=spark).read_file_from_hadoop(
        file_path
    )
    assert config_string == expected


def test_get_config_from_hacon(spark, repo_root) -> None:
    file_path = Path(
        repo_root / "tests/resources/test_metastore.conf"
    ).as_posix()
    metastore_tables = FileSystemUtils(
        spark=spark
    ).load_hocon_config_from_hadoop(file_path)
    expected = [
        MetastoreTable(
            name="lookup",
            format="parquet",
            info_date_settings=InfoDateSettings(
                column="", format="yyyy-MM-dd", start=datetime.date(2022, 1, 1)
            ),
            path="test4/lookup",
            table="",
            description="A lookup table",
            records_per_partition=500000,
        ),
        MetastoreTable(
            name="lookup2",
            format="parquet",
            info_date_settings=InfoDateSettings(
                column="", format="yyyy-MM-dd", start=None
            ),
            path="test4/lookup2",
            table="",
            description="",
            records_per_partition=500000,
        ),
        MetastoreTable(
            name="users1",
            format="parquet",
            info_date_settings=InfoDateSettings(
                column="", format="yyyy-MM-dd", start=datetime.date(2022, 1, 1)
            ),
            path="test4/users1",
            table="",
            description="Test users 2",
            records_per_partition=500000,
        ),
        MetastoreTable(
            name="users3",
            format="delta",
            info_date_settings=InfoDateSettings(
                column="sync_watcher_date",
                format="yyyy-MM-dd",
                start=datetime.date(2022, 1, 1),
            ),
            path="test4/users3",
            table="",
            description="Test users 3",
            records_per_partition=500000,
        ),
        MetastoreTable(
            name="users4",
            format="delta",
            info_date_settings=InfoDateSettings(
                column="", format="yyyy-MM-dd", start=datetime.date(2022, 1, 1)
            ),
            path="test4/users4",
            table="",
            description="Test users 4",
            records_per_partition=500000,
        ),
        MetastoreTable(
            name="teller",
            format="delta",
            info_date_settings=InfoDateSettings(
                column="", format="yyyy-MM-dd", start=None
            ),
            path="transactions_new5/teller",
            table="",
            description="",
            records_per_partition=500000,
        ),
    ]
    assert metastore_tables == expected
