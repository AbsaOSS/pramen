from typing import List

from pramen_py.models import WatcherMetastoreTable


def get_metastore_table(
    table_name: str,
    tables: List[WatcherMetastoreTable],
) -> WatcherMetastoreTable:
    def filter_func(table: WatcherMetastoreTable) -> bool:
        return table.name == table_name

    try:
        return next(filter(filter_func, tables))
    except StopIteration as err:
        raise KeyError(
            f"Table {table_name} missed in the config. "
            f"Available tables are:\n"
            f"{chr(10).join(t.name for t in tables)}"
        ) from err
