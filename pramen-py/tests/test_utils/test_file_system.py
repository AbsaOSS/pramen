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


def test_fix_uri_scheme_availability() -> None:
    test_paths = (
        {"input": "file://C:/Users/AB025F5/example_dependency_table",
         "output": "file://C:/Users/AB025F5/example_dependency_table"},
        {"input": "s3a://C:/Users/AB025F5/example_dependency_table",
         "output": "s3a://C:/Users/AB025F5/example_dependency_table"},
        {"input": "hdfs://C:/Users/AB025F5/example_dependency_table",
         "output": "hdfs://C:/Users/AB025F5/example_dependency_table"},
        {"input": "hdfs://Users/AB025F5/example_dependency_table",
         "output": "hdfs://Users/AB025F5/example_dependency_table"},
        {"input": "//C:/Users/AB025F5/example_dependency_table",
         "output": "file://C:/Users/AB025F5/example_dependency_table"},
        {"input": "//Users/AB025F5/example_dependency_table",
         "output": "file://Users/AB025F5/example_dependency_table"},
        {"input": "/Users/AB025F5/example_dependency_table",
         "output": "file://Users/AB025F5/example_dependency_table"},
        {"input": "Users/AB025F5/example_dependency_table",
         "output": "file://Users/AB025F5/example_dependency_table"},
        {"input": "//D:/Users/AB025F5/example_dependency_table",
         "output": "file://D:/Users/AB025F5/example_dependency_table"}
    )
    test_spark = SparkSession.builder.master("local[1]") \
        .appName('SparkByExamples.com') \
        .getOrCreate()
    test_fs_utils = FileSystemUtils(spark=test_spark)
    for path in test_paths:
        assert test_fs_utils.fix_uri_scheme_availability(path["input"]) == path["output"]
