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

import os.path

from typing import List

import attrs

from pyspark.sql import SparkSession
from typing_extensions import Protocol


@attrs.define(auto_attribs=True, slots=True)
class FileSystemUtils:
    """FileSystem agnostic set of utilities.

    Uses org.apache.hadoop.fs.FileSystem Java library under the hood
    https://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/fs/FileSystem.html

    Typing hints for java objects are created independently with help of
    typing.Protocol.
    """

    spark: SparkSession = attrs.field()

    URI: "_URI" = attrs.field(init=False)
    Path: "_Path" = attrs.field(init=False)
    FileSystem: "_FileSystem" = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        sc = self.spark.sparkContext
        self.URI = sc._gateway.jvm.java.net.URI  # type: ignore
        self.Path = sc._gateway.jvm.org.apache.hadoop.fs.Path  # type: ignore
        self.FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem  # type: ignore

    def get_fs_from_uri(self, uri: str) -> "_FileSystem":
        """Get Type[FileSystem] object from the uri.

        Depending on uri will be using corresponding FileSystem subtype
        object (i.e. S3FileSystem, LocalFileSystem etc.)
        """
        return self.FileSystem.get(
            self.URI(uri),
            self.spark.sparkContext._jsc.hadoopConfiguration(),  # type: ignore
        )

    def list_files(
        self,
        uri: str,
        file_pattern: str = "*",
    ) -> List[str]:
        """List files in a given uri, filtered by a glob pattern.

        file_pattern is used to filter file names. i.e.
        `information_date=*` will provide only files which names are
        starting from information_date=...
        """
        fs = self.get_fs_from_uri(uri)
        return [
            f.getPath().toString()
            for f in fs.globStatus(
                self.Path(os.path.join(uri, "") + file_pattern)
            )
        ]


# Typing info for py4j underlying objects
class _FileSystem(Protocol):
    def get(self, uri: "_URI", hdfs_config: object) -> "_FileSystem":
        ...

    def globStatus(self, path: "_Path") -> List["_File"]:
        ...


class _URI(Protocol):
    def __call__(self, s: str) -> "_URI":
        ...


class _Path(Protocol):
    def __call__(self, glob: str) -> "_Path":
        ...


class _File(Protocol):
    def getPath(self) -> "_HasToString":
        ...


class _HasToString(Protocol):
    def toString(self) -> str:
        ...
