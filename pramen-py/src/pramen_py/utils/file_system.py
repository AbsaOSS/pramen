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
import re

from typing import List

import attrs

from pyhocon import ConfigFactory, ConfigTree  # type: ignore
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
    IOUtils: "_IOUtils" = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        sc = self.spark.sparkContext
        self.URI = sc._gateway.jvm.java.net.URI  # type: ignore
        self.Path = sc._gateway.jvm.org.apache.hadoop.fs.Path  # type: ignore
        self.FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem  # type: ignore
        self.IOUtils = sc._gateway.jvm.org.apache.commons.io.IOUtils  # type: ignore

    def get_fs_from_uri(self, uri: str) -> "_FileSystem":
        """Get Type[FileSystem] object from the uri.

        Depending on uri will be using corresponding FileSystem subtype
        object (i.e. S3FileSystem, LocalFileSystem etc.)
        """
        return self.FileSystem.get(
            self.URI(self.ensure_proper_schema_for_local_fs(uri)),
            self.spark.sparkContext._jsc.hadoopConfiguration(),  # type: ignore
        )

    @staticmethod
    def ensure_proper_schema_for_local_fs(uri: str) -> str:
        """Ensure schema for local paths

        org.apache.hadoop.fs.FileSystem can`t read uri without schema.
        an example 'C:/somepath'. It parsed 'C:' like schema. This leads to errors.
        LocalFileSystem Path should be: 'file:///C:/somepath'.
        Based on pathlib._WindowsFlavour(_Flavour).make_uri() principles
        """
        uri_parts = list(filter(None, re.split(r"\\|/", uri)))
        if len(uri_parts) >= 1:
            drive = uri_parts[0]
            if len(drive) == 2 and drive[0].isalpha() and drive[1] == ":":
                return f"file:///{'/'.join(uri_parts)}"
            else:
                return uri.replace("\\", "/")
        return uri.replace("\\", "/")

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

    def load_and_read_file_from_hadoop(self, path: str) -> str:
        """Read file by path from hadoop.

        :param path: path to file in hadoop file system
        """
        fs = self.get_fs_from_uri(path)
        stream = fs.open(self.Path(path))
        config_string = self.IOUtils.toString(stream, "UTF-8")  # type: ignore
        fs.close()
        return config_string

    def load_hocon_config_from_hadoop(self, path: str) -> ConfigTree:
        """Read and parse hacon config file from hadoop file system .

        :param path: path to file in hadoop file system
        """
        return ConfigFactory.parse_string(
            self.load_and_read_file_from_hadoop(path)
        )


# Typing info for py4j underlying objects
class _FileSystem(Protocol):
    def get(self, uri: "_URI", hdfs_config: object) -> "_FileSystem":
        ...

    def globStatus(self, path: "_Path") -> List["_File"]:
        ...

    def close(self) -> None:
        ...

    def open(self, f: "_Path") -> "_FSDataInputStream":
        ...


class _URI(Protocol):
    def __call__(self, s: str) -> "_URI":
        ...


class _Path(Protocol):
    def __call__(self, glob: str) -> "_Path":
        ...


class _IOUtils(Protocol):
    def __call__(self) -> "_IOUtils":
        ...

    def toString(self, stream: "_InputStream", charset: str) -> str:
        ...


class _FSDataInputStream(Protocol):
    def __call__(self, stream: "_InputStream") -> "_FSDataInputStream":
        ...


class _InputStream(Protocol):
    def __call__(self) -> "_InputStream":
        ...


class _File(Protocol):
    def getPath(self) -> "_HasToString":
        ...


class _HasToString(Protocol):
    def toString(self) -> str:
        ...
