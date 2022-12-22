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
            "/User",
            "/User",
        ),
        (
            "User",
            "User",
        ),
        (
            "C:",
            "file:///C:",
        ),
        (
            "C:\\User",
            "file:///C:/User",
        ),
        (
            "hdfs:///Users/AB025F5/example_dependency_table",
            "hdfs:///Users/AB025F5/example_dependency_table",
        ),
        (
            "Users/AB025F5/example_dependency_table",
            "Users/AB025F5/example_dependency_table",
        ),
        (
            "Users/AB025F5/example_dependency_table/text.txt",
            "Users/AB025F5/example_dependency_table/text.txt",
        ),
        (
            "file:///Users/AB025F5/example_dependency_table",
            "file:///Users/AB025F5/example_dependency_table",
        ),
        (
            "D:/Users/AB025F5//example_dependency_table/text.txt",
            "file:///D:/Users/AB025F5/example_dependency_table/text.txt",
        ),
        (
            "file:\\\\\\D:\\Users\\AB025F5\\example_dependency_table",
            "file:///D:/Users/AB025F5/example_dependency_table",
        ),
        (
            "D:\\Users\\AB025F5\\example_dependency_table",
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
    config_string = (
        FileSystemUtils(spark=spark)
        .load_and_read_file_from_hadoop(file_path)
        .replace("\r\n", "\n")
    )

    assert config_string == (
        "pramen {\n"
        '  environment.name = "TestEnv"\n'
        "  bookkeeping.enabled = true\n"
        "  bookkeeping.jdbc {\n"
        '    driver = "org.postgresql.Driver"\n'
        '    url = "jdbc:postgresql://127.0.0.1:8080/newpipeline"\n'
        '    user = "user"\n'
        "  }\n"
        "  warn.throughput.rps = 2000\n"
        "  good.throughput.rps = 40000\n"
        "}\n"
        "mail {\n"
        '  smtp.host = "127.0.0.1"\n'
        '  send.from = "Pramen <noreply@example.com>"\n'
        '  send.to = "user@example.com"\n'
        "}\n"
        'sourcing.base.path = "/projects/batch/source"\n'
    )


def test_load_hocon_config_from_hadoop(spark, repo_root) -> None:
    file_path = PurePath(
        repo_root / "tests/resources/test_metastore.conf"
    ).as_posix()
    hocon_config = FileSystemUtils(spark=spark).load_hocon_config_from_hadoop(
        file_path
    )
    tables = hocon_config.get("pramen.metastore.tables", "")

    assert len(tables) == 6
    assert tables[3].get("table", "") == "users3"
    assert tables[5].get("table", "") == "teller"
    table_0 = tables[0]
    assert table_0.get("name", "") == "lookup"
    assert table_0.get("format", "") == "delta"
    assert table_0.get("description", "") == "A lookup table"
    assert table_0.get("path", "") == "test4/lookup"
    assert table_0.get("records_per_partition", "") == 10000
    assert table_0.get("information.date.column", "") == "information_date"
    assert table_0.get("information.date.format", "") == "yyyy-MM-dd"
    assert table_0.get("information.date.start", "") == "2022-01-01"
