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
from pyspark.sql import SparkSession
from pramen_py.utils.file_system import FileSystemUtils
from loguru import logger

@pytest.mark.parametrize(
    ("inputs", "outputs"),
    (
        ("file://C:/Users/AB025F5/example_dependency_table",
         "file://C:/Users/AB025F5/example_dependency_table"),
        ("s3a://C:/Users/AB025F5/example_dependency_table",
         "s3a://C:/Users/AB025F5/example_dependency_table"),
        ("hdfs://C:/Users/AB025F5/example_dependency_table",
         "hdfs://C:/Users/AB025F5/example_dependency_table"),
        ("hdfs://Users/AB025F5/example_dependency_table",
         "hdfs://Users/AB025F5/example_dependency_table"),
        ("//C:/Users/AB025F5/example_dependency_table",
         "file://C:/Users/AB025F5/example_dependency_table"),
        ("//Users/AB025F5/example_dependency_table",
         "file://Users/AB025F5/example_dependency_table"),
        ("/Users/AB025F5/example_dependency_table",
         "file://Users/AB025F5/example_dependency_table"),
        ("Users/AB025F5/example_dependency_table",
         "file://Users/AB025F5/example_dependency_table"),
        ("//D:/Users/AB025F5/example_dependency_table",
         "file://D:/Users/AB025F5/example_dependency_table")
    ),
)
def test_ensure_proper_schema_for_local_fs(spark, inputs, outputs) -> None:
    test_fs_utils = FileSystemUtils(spark=spark)
    assert test_fs_utils.ensure_proper_schema_for_local_fs(inputs) == outputs
