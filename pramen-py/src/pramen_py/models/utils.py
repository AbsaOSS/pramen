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


from typing import List
from pyhocon import ConfigFactory

from pramen_py.models import InfoDateSettings, MetastoreTable


def get_metastore_table(
    table_name: str,
    tables: List[MetastoreTable],
) -> MetastoreTable:
    def filter_func(table: MetastoreTable) -> bool:
        return table.name == table_name

    try:
        return next(filter(filter_func, tables))
    except StopIteration as err:
        raise KeyError(
            f"Table {table_name} missed in the config. "
            f"Available tables are:\n"
            f"{chr(10).join(t.name for t in tables)}"
        ) from err


def load_config_from_hadoop(
        includes: str,
) -> List[MetastoreTable]:
    config = ConfigFactory.parse_string("""
        include "C:/data/common.conf" 
        include "C:/data/metastore.conf"
    """)
    tables = config.get("pramen.metastore.tables")
    metastore_tables = []
    for table in tables:
        metastore = MetastoreTable(
            name=table.get("name", ""),
            format=table.get("format", ""),
            path=table.get("path", ""),
            table=table.get("table", ""),
            description=table.get("description", ""),
            records_per_partition=table.get("records_per_partition", 500000),
            info_date_settings=InfoDateSettings(
                column=table.get("information.date.column", ""),
                format=table.get("information.date.format", "yyyy-MM-dd"),
                start=table.get("information.date.start", None),
            ),
        )
        metastore_tables.append(metastore)
    return metastore_tables

