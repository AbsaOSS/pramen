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

import pytest

from pramen_py.utils.file_system import FileSystemUtils
from pramen_py.metastore.reader import MetastoreReader
from pramen_py.models import utils

@pytest.mark.parametrize(
    ("inputs", "outputs"),
    (
        (
            "file://C:/Users/AB025F5/example_dependency_table",
            "file://C:/Users/AB025F5/example_dependency_table",
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
            "file://C:/Users/AB025F5/example_dependency_table",
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
            "//D:/Users/AB025F5/example_dependency_table//",
            "file://D:/Users/AB025F5/example_dependency_table",
        ),
        (
            "D:\\Users\\AB025F5\\example_dependency_table\\",
            "file://D:/Users/AB025F5/example_dependency_table",
        ),
    ),
)
def test_ensure_proper_schema_for_local_fs(spark, inputs, outputs) -> None:
    test_fs_utils = FileSystemUtils(spark=spark)
    assert test_fs_utils.ensure_proper_schema_for_local_fs(inputs) == outputs


def test_load_config_from_hadoop(spark) -> None:
    #// s3:///a/b/c , hdfs://server/path/file /some/path file:///tmp/file.txt
    fsu = FileSystemUtils(spark)
    res = fsu.read_file_from_hadoop("C:\\data\\main.conf")

    config = utils.load_config_from_hadoop(res)
    info_date = "2022-03-23"
    metastore = MetastoreReader(spark, config, info_date)

    b = "C:\\data\\common.conf"
    c = "C:\\data\\metastore.conf"

    return