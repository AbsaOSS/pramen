/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.job.v2.job

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.scalatest.WordSpec
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.api.metastore.MetaTableStats
import za.co.absa.pramen.framework.OperationDefFactory
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.exceptions.ProcessFailedException
import za.co.absa.pramen.framework.fixtures.TextComparisonFixture
import za.co.absa.pramen.framework.mocks.MetaTableFactory
import za.co.absa.pramen.framework.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.framework.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.framework.mocks.process.ProcessRunnerSpy

import java.nio.file.{Files, Paths}
import java.time.{Instant, LocalDate}

class PythonTransformationJobSuite extends WordSpec with SparkTestBase with TextComparisonFixture {

  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  private val conf: Config = ConfigFactory.parseString(
    s"""
       |//pramen.sources = [ ]
       |pramen.metastore {
       |  tables = [
       |    {
       |      name = "table1"
       |      format = "parquet"
       |      path = /dummy/table1
       |    },
       |    {
       |      name = "table2"
       |      format = "delta"
       |      path = /dummy/table2
       |    }
       |  ]
       |}
       |
       |pramen.pipeline {
       |  name = "pipeline_v2"
       |}
       |
       |pramen.operations = [
       |  {
       |    name = "Python transformer"
       |    type = "python_transformer"
       |    python.class = "python1"
       |    schedule.type = "daily"
       |
       |    output.table = "table2"
       |
       |    dependencies = [
       |      {
       |        tables = [ table1 ]
       |        date.from = "@infoDate"
       |      }
       |    ]
       |
       |    option {
       |      key1 = "value2"
       |      key2 = "value2"
       |    }
       |  }
       |]
       |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  "preRunCheckJob" should {
    "always return Ready" in {
      val (_, _, job, _) = getUseCase()

      val result = job.preRunCheckJob(infoDate, conf, Nil)

      assert(result.status == JobPreRunStatus.Ready)
    }
  }

  "validate" should {
    "return Ready" in {
      val (_, _, job, _) = getUseCase()

      val result = job.validate(infoDate, conf)

      assert(result == Reason.Ready)
    }
  }

  "run" should {
    "run the command line script" in {
      val (_, _, job, runner) = getUseCase(tableDf = exampleDf)

      val df = job.run(infoDate, conf)

      assert(df.count() == 3)
      assert(runner.runCommands.length == 1)
      assert(runner.runCommands.head == "/dummy/path/dummy_cmd transformations run python_class")
    }

    "throw an exception if the command throws an exception" in {
      val (_, _, job, _) = getUseCase(tableDf = exampleDf, runException = new IllegalStateException(s"Dummy exception"))

      val ex = intercept[RuntimeException] {
        job.run(infoDate, conf)
      }

      assert(ex.getCause.getMessage == "Dummy exception")
    }

    "throw an exception if output table is not created in the metastore" in {
      val (_, _, job, _) = getUseCase(tableDf = exampleDf, tableException = new NoSuchDatabaseException("Dummy"))

      val ex = intercept[RuntimeException] {
        job.run(infoDate, conf)
      }

      assert(ex.getMessage.contains("Output data not found in the metastore"))
      assert(ex.getCause.getMessage.contains("Dummy"))
    }
  }

  "postProcessing" should {
    "return the original dataframe" in {
      val (_, _, job, _) = getUseCase()

      val df = job.postProcessing(exampleDf, infoDate, conf)
      assert(df.count() == 3)
    }
  }

  "save" should {
    "update the bookkeeper" in {
      val statsIn = MetaTableStats(100, None)

      val (_, _, job, _) = getUseCase(stats = statsIn)

      val started = Instant.ofEpochSecond(12345678L)

      val statsOut = job.save(exampleDf, infoDate, conf, started, None)

      assert(statsOut == statsIn)
    }

    "throw an exception if metastore throws AnalysisException" in {
      val (_, _, job, _) = getUseCase(statsException = new NoSuchDatabaseException("Dummy"))

      val started = Instant.ofEpochSecond(12345678L)

      val ex = intercept[RuntimeException] {
        job.save(exampleDf, infoDate, conf, started, None)
      }

      assert(ex.getMessage.contains("Output data not found in the metastore"))
    }

    "allow no records in the output table" in {
      val statsIn = MetaTableStats(0, None)

      val (_, _, job, _) = getUseCase(stats = statsIn)

      val started = Instant.ofEpochSecond(12345678L)

      val statsOut = job.save(exampleDf, infoDate, conf, started, None)

      assert(statsOut.recordCount == 0)
    }

    "throw an exception if no records in the output table" in {
      val statsIn = MetaTableStats(0, None)

      val (_, _, job, _) = getUseCase(stats = statsIn, extraOptions = Map("minimum.records" -> "1"))

      val started = Instant.ofEpochSecond(12345678L)

      val ex = intercept[RuntimeException] {
        job.save(exampleDf, infoDate, conf, started, None)
      }

      assert(ex.getMessage.contains("Output table is empty in the metastore"))
    }

    "throw an exception if the number of records is less then expected" in {
      val statsIn = MetaTableStats(9, None)

      val (_, _, job, _) = getUseCase(stats = statsIn, extraOptions = Map("minimum.records" -> "10"))

      val started = Instant.ofEpochSecond(12345678L)

      val ex = intercept[RuntimeException] {
        job.save(exampleDf, infoDate, conf, started, None)
      }

      assert(ex.getMessage.contains("The transformation returned too few records (9 < 10)"))
    }

    "throw an exception if metastore throws any other exception" in {
      val (_, _, job, _) = getUseCase(statsException = new IllegalStateException("Dummy"))

      val started = Instant.ofEpochSecond(12345678L)

      val ex = intercept[RuntimeException] {
        job.save(exampleDf, infoDate, conf, started, None)
      }

      assert(ex.getMessage.contains("Dummy"))
    }
  }

  "runPythonCmdLine" should {
    "run the proper command line" in {
      val (_, _, job, runner) = getUseCase()

      job.runPythonCmdLine(infoDate, conf)

      assert(runner.runCommands.length == 1)
      assert(runner.runCommands.head == "/dummy/path/dummy_cmd transformations run python_class")
    }

    "throws RuntimeException if the command throws an exception" in {
      val (_, _, job, _) = getUseCase(runException = new IllegalStateException("Dummy"))

      val ex = intercept[RuntimeException] {
        job.runPythonCmdLine(infoDate, conf)
      }

      assert(ex.getMessage.contains("The process has exited with an exception"))
      assert(ex.getCause.getMessage.contains("Dummy"))
    }

    "throws ProcessFailedException for non-zero exit code" in {
      val (_, _, job, _) = getUseCase(exitCode = 1)

      val ex = intercept[ProcessFailedException] {
        job.runPythonCmdLine(infoDate, conf)
      }

      assert(ex.getMessage == "The process has exited with error code 1.")
    }
  }

  "getMetastoreConfig" should {
    val expected =
      """run_transformers:
        |- info_date: 2022-02-18
        |  output_table: table2
        |  name: python_class
        |  options: {}
        |pramen_metastore_tables:
        |- name: table1
        |  description: description
        |  format: parquet
        |  path: /tmp/dummy
        |  records_per_partition: 500000
        |  info_date_settings:
        |    column: INFO_DATE
        |    format: yyyy-MM-dd
        |    start: 2020-01-31
        |- name: table2
        |  description: description
        |  format: parquet
        |  path: /tmp/dummy
        |  records_per_partition: 500000
        |  info_date_settings:
        |    column: INFO_DATE
        |    format: yyyy-MM-dd
        |    start: 2020-01-31
        |""".stripMargin

    "creates a temporary config" in {
      val (_, _, job, _) = getUseCase()

      val tempFile = job.getMetastoreConfig(infoDate, conf)

      val actual = Files.readAllLines(Paths.get(tempFile)).toArray.mkString("\n")

      compareText(actual, expected)
    }
  }

  "getYamlConfig" should {
    "generate yaml config from the metastore and task definition" in {
      val expected =
        """run_transformers:
          |- info_date: 2022-02-18
          |  output_table: table2
          |  name: python_class
          |  options: {}
          |pramen_metastore_tables:
          |- name: table1
          |  description: description
          |  format: parquet
          |  path: /tmp/dummy
          |  records_per_partition: 500000
          |  info_date_settings:
          |    column: INFO_DATE
          |    format: yyyy-MM-dd
          |    start: 2020-01-31
          |- name: table2
          |  description: description
          |  format: parquet
          |  path: /tmp/dummy
          |  records_per_partition: 500000
          |  info_date_settings:
          |    column: INFO_DATE
          |    format: yyyy-MM-dd
          |    start: 2020-01-31
          |""".stripMargin

      val (_, _, job, _) = getUseCase()

      val actual = job.getYamlConfig(infoDate, conf)

      compareText(actual, expected)
    }
    "generate yaml config from the metastore and task definition with extra options" in {
      val expected =
        """run_transformers:
          |- info_date: 2022-02-18
          |  output_table: table2
          |  name: python_class
          |  options:
          |    key.2: "value'2'"
          |    key1: "value1"
          |pramen_metastore_tables:
          |- name: table1
          |  description: description
          |  format: parquet
          |  path: /tmp/dummy
          |  records_per_partition: 500000
          |  info_date_settings:
          |    column: INFO_DATE
          |    format: yyyy-MM-dd
          |    start: 2020-01-31
          |- name: table2
          |  description: description
          |  format: parquet
          |  path: /tmp/dummy
          |  records_per_partition: 500000
          |  info_date_settings:
          |    column: INFO_DATE
          |    format: yyyy-MM-dd
          |    start: 2020-01-31
          |""".stripMargin

      val (_, _, job, _) = getUseCase(extraOptions = Map[String, String]("key1" -> "value1", "key.2" -> "value'2'"))

      val actual = job.getYamlConfig(infoDate, conf)

      compareText(actual, expected)
    }
  }

  def getUseCase(exitCode: Int = 0,
                 tableDf: DataFrame = null,
                 runException: Throwable = null,
                 tableException: Throwable = null,
                 stats: MetaTableStats = null,
                 statsException: Throwable = null,
                 extraOptions: Map[String, String] = Map.empty[String, String]): (SyncBookkeeperMock, MetastoreSpy, PythonTransformationJob, ProcessRunnerSpy) = {
    val bk = new SyncBookkeeperMock
    val metastore = new MetastoreSpy(tableDf = tableDf, tableException = tableException, stats = stats, statsException = statsException)
    val operationDef = OperationDefFactory.getDummyOperationDef(extraOptions = extraOptions)

    val outputTable = MetaTableFactory.getDummyMetaTable(name = "table2")

    val syncWatcherPyConfig = SyncWatcherPyConfig("/dummy/path", "dummy_cmd", "@location/@executable transformations run @pythonClass")

    val processRunner = new ProcessRunnerSpy(exitCode, runException = runException)

    val job = new PythonTransformationJob(
      operationDef,
      metastore,
      bk,
      outputTable,
      "python_class",
      syncWatcherPyConfig,
      processRunner
    )

    (bk, metastore, job, processRunner)
  }
}
