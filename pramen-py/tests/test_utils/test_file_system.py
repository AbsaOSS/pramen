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

from pathlib import PurePath

import pytest

from pyhocon import ConfigTree

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


def test_load_and_read_file_from_hadoop(spark, repo_root) -> None:
    file_path = PurePath(
        repo_root / "tests/resources/test_common.conf"
    ).as_posix()
    config_string = FileSystemUtils(
        spark=spark
    ).load_and_read_file_from_hadoop(file_path)
    assert config_string == (
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
    )


def test_load_hocon_config_from_hadoop(spark, repo_root) -> None:
    file_path = PurePath(
        repo_root / "tests/resources/test_metastore.conf"
    ).as_posix()
    hocon_config = FileSystemUtils(spark=spark).load_hocon_config_from_hadoop(
        file_path
    )
    assert hocon_config == ConfigTree(
        [
            (
                "pramen",
                ConfigTree(
                    [
                        (
                            "metastore",
                            ConfigTree(
                                [
                                    (
                                        "tables",
                                        [
                                            ConfigTree(
                                                [
                                                    ("name", "lookup"),
                                                    (
                                                        "description",
                                                        "A lookup table",
                                                    ),
                                                    ("format", "parquet"),
                                                    ("path", "test4/lookup"),
                                                    (
                                                        "information",
                                                        ConfigTree(
                                                            [
                                                                (
                                                                    "date",
                                                                    ConfigTree(
                                                                        [
                                                                            (
                                                                                "start",
                                                                                "2022-01-01",
                                                                            )
                                                                        ]
                                                                    ),
                                                                )
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                            ConfigTree(
                                                [
                                                    ("name", "lookup2"),
                                                    ("format", "parquet"),
                                                    ("path", "test4/lookup2"),
                                                ]
                                            ),
                                            ConfigTree(
                                                [
                                                    ("name", "users1"),
                                                    (
                                                        "description",
                                                        "Test users 2",
                                                    ),
                                                    ("format", "parquet"),
                                                    ("path", "test4/users1"),
                                                    (
                                                        "information",
                                                        ConfigTree(
                                                            [
                                                                (
                                                                    "date",
                                                                    ConfigTree(
                                                                        [
                                                                            (
                                                                                "start",
                                                                                "2022-01-01",
                                                                            )
                                                                        ]
                                                                    ),
                                                                )
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                            ConfigTree(
                                                [
                                                    ("name", "users3"),
                                                    (
                                                        "description",
                                                        "Test users 3",
                                                    ),
                                                    ("format", "delta"),
                                                    ("path", "test4/users3"),
                                                    (
                                                        "information",
                                                        ConfigTree(
                                                            [
                                                                (
                                                                    "date",
                                                                    ConfigTree(
                                                                        [
                                                                            (
                                                                                "column",
                                                                                "sync_watcher_date",
                                                                            ),
                                                                            (
                                                                                "start",
                                                                                "2022-01-01",
                                                                            ),
                                                                        ]
                                                                    ),
                                                                )
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                            ConfigTree(
                                                [
                                                    ("name", "users4"),
                                                    (
                                                        "description",
                                                        "Test users 4",
                                                    ),
                                                    ("format", "delta"),
                                                    ("path", "test4/users4"),
                                                    (
                                                        "information",
                                                        ConfigTree(
                                                            [
                                                                (
                                                                    "date",
                                                                    ConfigTree(
                                                                        [
                                                                            (
                                                                                "start",
                                                                                "2022-01-01",
                                                                            )
                                                                        ]
                                                                    ),
                                                                )
                                                            ]
                                                        ),
                                                    ),
                                                ]
                                            ),
                                            ConfigTree(
                                                [
                                                    ("name", "teller"),
                                                    ("format", "delta"),
                                                    (
                                                        "path",
                                                        "transactions_new5/teller",
                                                    ),
                                                ]
                                            ),
                                        ],
                                    )
                                ]
                            ),
                        )
                    ]
                ),
            )
        ]
    )
