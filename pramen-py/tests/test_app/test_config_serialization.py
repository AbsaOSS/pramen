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
    for t in config["metastore_tables"]:
        try:
            t["records_per_partition"] = int(t["records_per_partition"])
        except KeyError:
            t["records_per_partition"] = 500000

    # if ExampleTransformation1 do not have the spark_config,
    # the field in structured variant is presented with the value None
    config["run_transformers"][0]["spark_config"] = {}

    # deserialize the config
    t_config_unstructured = cattr.unstructure(t_config_structured)

    for i in range(len(config["metastore_tables"])):
        if config["metastore_tables"][i].get("table"):
            t_config_unstructured["metastore_tables"][i].pop("path", None)
        elif config["metastore_tables"][i].get("path"):
            t_config_unstructured["metastore_tables"][i].pop("table", None)

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

    for i in range(len(config["metastore_tables"])):
        if config["metastore_tables"][i].get("table"):
            t_config_unstructured["metastore_tables"][i].pop("path", None)
        elif config["metastore_tables"][i].get("path"):
            t_config_unstructured["metastore_tables"][i].pop("table", None)

    assert t_config_unstructured == {
        "run_transformers": [
            {
                "name": "ExampleTransformation1",
                "info_date": "2022-02-14",
                "output_table": "table_out1",
                "spark_config": {},
                "options": {},
            },
            {
                "name": "ExampleTransformation2",
                "info_date": "2022-02-15",
                "output_table": "table_out1",
                "options": {},
                "spark_config": {
                    "spark.driver.host": "127.0.0.1",
                    "spark.executor.cores": "1",
                    "spark.executor.instances": "1",
                },
            },
        ],
        "metastore_tables": [
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
                "reader_options": {},
                "writer_options": {},
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
                "reader_options": {"mergeSchema": "false"},
                "writer_options": {"compression": "snappy"},
            },
        ],
    }
