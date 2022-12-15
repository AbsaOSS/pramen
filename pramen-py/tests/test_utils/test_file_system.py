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
        '    url = "jdbc:postgresql://127.0.0.1:8080/newpipeline"\n'
        '    user = "app_account"\n'
        "  }\n"
        "  warn.throughput.rps = 2000\n"
        "  good.throughput.rps = 40000\n"
        "}\n"
        "mail {\n"
        '  smtp.host = "127.0.0.1"\n'
        '  send.from = "Pramen <hmbrand.noreply@gmail.com>"\n'
        '  send.to = "crowl@gmail.com"\n'
        "}\n"
        'sourcing.base.path = "/projects/batch/source"\n'
    )


@pytest.mark.parametrize(
    ("key", "expected_value"),
    (
        (["pramen.metastore.tables", 0, "description"], "A lookup table"),
        (["pramen.metastore.tables", 1, "name"], "lookup2"),
        (["pramen.metastore.tables", 2, "path"], "test4/users1"),
        (
            ["pramen.metastore.tables", 3, "information.date.column"],
            "sync_watcher_date",
        ),
        (
            ["pramen.metastore.tables", 4, "information.date.start"],
            "2022-01-01",
        ),
        (["pramen.metastore.tables", 5, "format"], "delta"),
    ),
)
def test_load_hocon_config_from_hadoop(
    spark, repo_root, key, expected_value
) -> None:
    file_path = PurePath(
        repo_root / "tests/resources/test_metastore.conf"
    ).as_posix()
    hocon_config = FileSystemUtils(spark=spark).load_hocon_config_from_hadoop(
        file_path
    )
    assert hocon_config.get(key[0], "") != ""
    tables = hocon_config[key[0]]
    assert len(tables) > key[1]
    table = tables[key[1]]
    assert table.get(key[2], "") == expected_value
