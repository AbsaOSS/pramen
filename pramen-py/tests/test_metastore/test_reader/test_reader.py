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
from pathlib import PurePath

from pyhocon import ConfigFactory  # type: ignore

from pramen_py import MetastoreReader
from pramen_py.models import InfoDateSettings, MetastoreTable, TableFormat


def test_from_config(
    spark,
    repo_root,
) -> None:
    file_path = PurePath(
        repo_root / "tests/resources/test_metastore.conf"
    ).as_posix()
    hocon_config = ConfigFactory.parse_file(file_path)
    metastore = MetastoreReader(spark).from_config(hocon_config)

    assert len(metastore.tables) == 6
    assert metastore.tables[1].format == TableFormat.parquet
    assert metastore.tables[2].table == ""
    assert metastore.tables[5].table == "teller"
    assert metastore.tables[5].reader_options == {"mergeSchema": "false"}
    assert metastore.tables[5].writer_options == {"mergeSchema": "true"}
    assert metastore.tables[0] == MetastoreTable(
        name="lookup",
        format=TableFormat.delta,
        path="test4/lookup",
        table="",
        description="A lookup table",
        records_per_partition=10000,
        info_date_settings=InfoDateSettings(
            column="information_date",
            format="yyyy-MM-dd",
            start=d(2022, 1, 1),
        ),
    )
