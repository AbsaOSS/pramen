import json

import cattr
import yaml

from pramen_py.models import TransformationConfig


def test_config_structure_unstructure(repo_root, monkeypatch) -> None:
    monkeypatch.setenv("PRAMENPY_DEFAULT_FS", "local")
    with open(
        (repo_root / "tests/resources/real_config.yaml").as_posix()
    ) as config_f:
        config = yaml.load(config_f, Loader=yaml.BaseLoader)

    # serialize the config dict
    t_config_structured = cattr.structure(config, TransformationConfig)

    # normalize the config for further comparison with the one
    #  retrieved via deserialization of the t_config_structured
    for t in config["watcher_metastore_tables"]:
        try:
            t["records_per_partition"] = int(t["records_per_partition"])
        except KeyError:
            t["records_per_partition"] = 500000

    # deserialize the config
    t_config_unstructured = cattr.unstructure(t_config_structured)

    # ensure they are equal
    assert config == t_config_unstructured

    # ensure unstructured config is json serializable
    json.dumps(t_config_unstructured)


def test_structured_config_is_deserialized_properly(repo_root, monkeypatch):
    with open(
        (repo_root / "tests/resources/real_config.yaml").as_posix()
    ) as config_f:
        config = yaml.load(config_f, Loader=yaml.BaseLoader)

    t_config_structured = cattr.structure(config, TransformationConfig)

    # deserialize the config
    t_config_unstructured = cattr.unstructure(t_config_structured)
    assert t_config_unstructured == {
        "run_transformers": [
            {
                "name": "ExampleTransformation1",
                "info_date": "2022-02-14",
                "output_table": "table_out1",
                "options": {},
            },
            {
                "name": "ExampleTransformation2",
                "info_date": "2022-02-15",
                "output_table": "table_out1",
                "options": {},
            },
        ],
        "watcher_metastore_tables": [
            {
                "name": "table1_sync",
                "description": "Table 1 description",
                "format": "parquet",
                "path": "/tmp/dummy/table1",
                "info_date_settings": {
                    "column": "info_date",
                    "format": "yyyy-MM-dd",
                    "start": "2017-01-31",
                },
                "records_per_partition": 1000000,
            },
            {
                "name": "table_out1",
                "description": "Output table",
                "format": "parquet",
                "path": "/tmp/dummy/table_out1",
                "info_date_settings": {
                    "column": "INFORMATION_DATE",
                    "format": "yyyy-MM-dd",
                    "start": "2017-01-29",
                },
                "records_per_partition": 500000,
            },
        ],
    }
