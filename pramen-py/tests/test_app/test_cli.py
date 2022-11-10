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
import platform

import pytest

from chispa import assert_column_equality, assert_df_equality
from click.testing import CliRunner
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import DateType

from pramen_py import MetastoreReader
from pramen_py.app.cli import main


def test_transformations_run_example_transformation1(
    load_and_patch_config,
    spark: SparkSession,
    repo_root,
    generate_df,
):
    """Run ExampleTransformation1 with the config and check output table."""

    runner = CliRunner(mix_stderr=False)
    results = runner.invoke(
        main,
        [
            "-v",
            "transformations",
            "run",
            "ExampleTransformation1",
            "--config",
            (repo_root / "tests/resources/real_config.yaml").as_posix(),
        ],
        catch_exceptions=False,
    )

    assert results.exit_code == 0

    # check if table was written indeed and its properties
    actual = spark.read.parquet(
        load_and_patch_config.metastore_tables[-1].path
    )
    expected = generate_df(
        """
        +---+---+----------+----------------+
        |A  |B  |info_date |INFORMATION_DATE|
        +---+---+----------+----------------+
        |7  |8  |2022-03-24|2022-02-14      |
        |9  |10 |2022-03-25|2022-02-14      |
        |11 |12 |2022-03-25|2022-02-14      |
        |5  |6  |2022-03-24|2022-02-14      |
        |13 |14 |2022-03-26|2022-02-14      |
        |3  |4  |2022-03-23|2022-02-14      |
        |1  |2  |2022-03-23|2022-02-14      |
        |15 |16 |2022-03-26|2022-02-14      |
        +---+---+----------+----------------+
        """,
        """
        root
         |-- A: integer (nullable = true)
         |-- B: integer (nullable = true)
         |-- info_date: date (nullable = true)
         |-- INFORMATION_DATE: date (nullable = true)
        """,
    )
    assert_df_equality(
        actual,
        expected,
        ignore_row_order=True,
        ignore_column_order=True,
    )


def test_transformations_run_with_info_date(
    load_and_patch_config,
    spark: SparkSession,
    repo_root,
):
    """Run ExampleTransformation1 with the --info-date option.

    Ensure the info-date provided via cli takes precedence.
    """
    runner = CliRunner(mix_stderr=False)
    results = runner.invoke(
        main,
        [
            "-v",
            "transformations",
            "run",
            "ExampleTransformation1",
            "--config",
            (repo_root / "tests/resources/real_config.yaml").as_posix(),
            "--info-date",
            "2022-04-05",
        ],
        catch_exceptions=False,
    )

    assert results.exit_code == 0

    # check the info_date column
    output_table = spark.read.parquet(
        load_and_patch_config.metastore_tables[-1].path
    )
    output_table = output_table.withColumn(
        "expected_information_date",
        lit("2022-04-05").cast(DateType()),
    )
    assert_column_equality(
        output_table,
        "INFORMATION_DATE",
        "expected_information_date",
    )


@pytest.mark.xfail
def test_spark_config_precedence_in_transformation_2(repo_root, when, mocker):
    """Ensure spark_config in yaml config takes precedence.

    ExampleTransformation2 in its configuration has spark_config,
    which should take precedence over the default config.
    """
    (
        when(MetastoreReader, "get_table")
        .called_with("table1_sync")
        .then_return(mocker.Mock())
    )
    runner = CliRunner(mix_stderr=False)
    runner.invoke(
        main,
        [
            "-v",
            "transformations",
            "run",
            "ExampleTransformation2",
            "--config",
            (repo_root / "tests/resources/real_config.yaml").as_posix(),
            "--parse",
            "argument",
        ],
        catch_exceptions=False,
    )

    session = SparkSession.getActiveSession()
    assert session.conf.get("spark.driver.host") == "127.0.0.1"
    assert session.conf.get("spark.executor.instances") == "1"
    assert session.conf.get("spark.executor.cores") == "1"


def test_transformations_run_cli_options(load_and_patch_config, repo_root):
    """Check that exit code is 2 when calling ExampleTransformation2.

    ExampleTransformation2 has additional required cli_option -p.
    This test verifies that if called without this option the exit code will
    be 2 and text in stderr will be relevant.
    """
    runner = CliRunner(mix_stderr=False)
    results = runner.invoke(
        main,
        [
            "-v",
            "transformations",
            "run",
            "ExampleTransformation2",
            "--config",
            (repo_root / "tests/resources/real_config.yaml").as_posix(),
        ],
        catch_exceptions=False,
    )

    assert results.exit_code == 2
    assert "Error: Missing option '-p' / '--parse'" in results.stderr

    result = runner.invoke(
        main,
        [
            "-v",
            "transformations",
            "run",
            "ExampleTransformation2",
            "--config",
            (repo_root / "tests/resources/real_config.yaml").as_posix(),
            # We are adding required parse option, so all should be fine
            "--parse",
            "argument",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_transformations_ls_basic(monkeypatch):
    runner = CliRunner(mix_stderr=False)
    results = runner.invoke(
        main,
        [
            "-v",
            "transformations",
            "ls",
        ],
        catch_exceptions=False,
    )
    assert results.exit_code == 0
    assert set(results.stdout.split()) == {
        "ExampleTransformation1",
        "ExampleTransformation2",
        "IdentityTransformer",
    }


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="This test is incompatible with Windows OS",
)
def test_completions_zsh(monkeypatch):
    runner = CliRunner(mix_stderr=False)
    results = runner.invoke(
        main,
        [
            "-v",
            "completions",
            "zsh",
        ],
        catch_exceptions=False,
    )
    assert results.exit_code == 0


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="This test is incompatible with Windows OS",
)
def test_completions_bash(monkeypatch):
    runner = CliRunner(mix_stderr=False)
    results = runner.invoke(
        main,
        [
            "-v",
            "completions",
            "bash",
        ],
        catch_exceptions=False,
    )
    assert results.exit_code == 0


def test_list_config_options(monkeypatch):
    runner = CliRunner(mix_stderr=False)
    results = runner.invoke(
        main,
        [
            "list-configuration-options",
        ],
        catch_exceptions=False,
    )
    assert results.exit_code == 0
