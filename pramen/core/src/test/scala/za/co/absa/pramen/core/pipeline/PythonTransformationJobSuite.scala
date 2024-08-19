/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.core.pipeline

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.exceptions.ProcessFailedException
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.mocks.MetaTableFactory
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.databricks.DatabricksClientSpy
import za.co.absa.pramen.core.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.core.mocks.process.ProcessRunnerSpy

import java.nio.file.{Files, Paths}
import java.time.{Instant, LocalDate}

class PythonTransformationJobSuite extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with TempDirFixture with TextComparisonFixture {

  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)
  private var tempDir: String = _
  private val exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
  private val runReason: TaskRunReason = TaskRunReason.New

  private def getConf(tempDir: String): Config = ConfigFactory.parseString(
    s"""
       |pramen.temporary.directory="$tempDir"
       |
       |pramen.py {
       |  databricks {
       |    job {
       |      job_setting_1 = "spark-3.3.1"
       |    }
       |  }
       |}
       |
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

  lazy val conf: Config = getConf(tempDir)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = createTempDir("python_transformer")
  }


  override def afterAll(): Unit = {
    deleteDir(tempDir)
    super.afterAll()
  }


  "preRunCheckJob" should {
    "always return Ready" in {
      val (job, _, _, _) = getUseCase()

      val result = job.preRunCheckJob(infoDate, runReason, conf, Nil)

      assert(result.status == JobPreRunStatus.Ready)
    }
  }

  "validate" should {
    "return Ready" in {
      val (job, _, _, _) = getUseCase()

      val result = job.validate(infoDate, conf)

      assert(result == Reason.Ready)
    }
  }

  "run" should {
    "run the command line script" in {
      val (job, runner, _, _) = getUseCase(tableDf = exampleDf)

      val runResult = job.run(infoDate, conf)

      val df = runResult.data

      assert(df.count() == 3)
      assert(runner.runCommands.length == 1)
      assert(runner.runCommands.head == "/dummy/path/dummy_cmd transformations run python_class")
    }

    "run the transformation on databricks" in {
      val (job, _, _, databricksClientOpt) = getUseCase(
        tableDf = exampleDf,
        useDatabricks = true,
        useCommandLine = false
      )
      val databricksClient = databricksClientOpt.get

      job.run(infoDate, conf)

      val (contents, filename, overwrite) = databricksClient.createFileInvocations.head
      val expectedSubmittedJob = Map("job_setting_1" -> "spark-3.3.1")

      assert(databricksClient.createFileInvocations.length == 1)
      assert(contents.contains("run_transformers:"))
      assert(contents.contains("metastore_tables:"))
      assert(filename.contains("dbfs:/"))
      assert(overwrite)
      assert(databricksClient.runTransientJobInvocations.length == 1)
      assert(databricksClient.runTransientJobInvocations.head == expectedSubmittedJob)
    }

    "throw an exception if neither command line configuration or databricks client are available" in {
      val (job, _, _, _) = getUseCase(
        tableDf = exampleDf,
        useCommandLine = false,
        useDatabricks = false
      )

      val ex = intercept[RuntimeException] {
        job.run(infoDate, conf)
      }

      assert(ex.getMessage == "Neither command line options nor databricks client configured correctly for Pramen-Py.")
    }

    "throw an exception if the command throws an exception" in {
      val (job, _, _, _) = getUseCase(tableDf = exampleDf, runException = new IllegalStateException(s"Dummy exception"))

      val ex = intercept[RuntimeException] {
        job.run(infoDate, conf)
      }

      assert(ex.getCause.getMessage == "Dummy exception")
    }

    "throw an exception if output table is not created in the metastore" in {
      val (job, _, _, _) = getUseCase(tableDf = exampleDf, tableException = new NoSuchDatabaseException("Dummy"))

      val ex = intercept[RuntimeException] {
        job.run(infoDate, conf)
      }

      assert(ex.getMessage.contains("Output data not found in the metastore"))
      assert(ex.getCause.getMessage.contains("Dummy"))
    }
  }

  "postProcessing" should {
    "return the original dataframe" in {
      val (job, _, _, _) = getUseCase()

      val df = job.postProcessing(exampleDf, infoDate, conf)
      assert(df.count() == 3)
    }
  }

  "save" should {
    "update the bookkeeper" in {
      val statsIn = MetaTableStats(100, None)

      val (job, _, _, _) = getUseCase(stats = statsIn)

      val started = Instant.ofEpochSecond(12345678L)

      val statsOut = job.save(exampleDf, infoDate, conf, started, None).stats

      assert(statsOut == statsIn)
    }

    "throw an exception if metastore throws AnalysisException" in {
      val (job, _, _, _) = getUseCase(statsException = new NoSuchDatabaseException("Dummy"))

      val started = Instant.ofEpochSecond(12345678L)

      val ex = intercept[RuntimeException] {
        job.save(exampleDf, infoDate, conf, started, None)
      }

      assert(ex.getMessage.contains("Output data not found in the metastore"))
    }

    "allow no records in the output table" in {
      val statsIn = MetaTableStats(0, None)

      val (job, _, _, _) = getUseCase(stats = statsIn)

      val started = Instant.ofEpochSecond(12345678L)

      val statsOut = job.save(exampleDf, infoDate, conf, started, None).stats

      assert(statsOut.recordCount == 0)
    }

    "throw an exception if no records in the output table" in {
      val statsIn = MetaTableStats(0, None)

      val (job, _, _, _) = getUseCase(stats = statsIn, extraOptions = Map("minimum.records" -> "1"))

      val started = Instant.ofEpochSecond(12345678L)

      val ex = intercept[RuntimeException] {
        job.save(exampleDf, infoDate, conf, started, None)
      }

      assert(ex.getMessage.contains("Output table is empty in the metastore"))
    }

    "throw an exception if the number of records is less then expected" in {
      val statsIn = MetaTableStats(9, None)

      val (job, _, _, _) = getUseCase(stats = statsIn, extraOptions = Map("minimum.records" -> "10"))

      val started = Instant.ofEpochSecond(12345678L)

      val ex = intercept[RuntimeException] {
        job.save(exampleDf, infoDate, conf, started, None)
      }

      assert(ex.getMessage.contains("The transformation returned too few records (9 < 10)"))
    }

    "throw an exception if metastore throws any other exception" in {
      val (job, _, _, _) = getUseCase(statsException = new IllegalStateException("Dummy"))

      val started = Instant.ofEpochSecond(12345678L)

      val ex = intercept[RuntimeException] {
        job.save(exampleDf, infoDate, conf, started, None)
      }

      assert(ex.getMessage.contains("Dummy"))
    }
  }

  "runPythonCmdLine" should {
    "run the proper command line" in {
      val (job, runner, pramenPyConfigOpt, _) = getUseCase()

      job.runPythonCmdLine(pramenPyConfigOpt.get, infoDate, conf)

      assert(runner.runCommands.length == 1)
      assert(runner.runCommands.head == "/dummy/path/dummy_cmd transformations run python_class")
    }

    "throws RuntimeException if the command throws an exception" in {
      val (job, _, pramenPyConfigOpt, _) = getUseCase(runException = new IllegalStateException("Dummy"))

      val ex = intercept[RuntimeException] {
        job.runPythonCmdLine(pramenPyConfigOpt.get, infoDate, conf)
      }

      assert(ex.getMessage.contains("The process has exited with an exception"))
      assert(ex.getCause.getMessage.contains("Dummy"))
    }

    "throws ProcessFailedException for non-zero exit code" in {
      val (job, _, pramenPyConfigOpt, _) = getUseCase(exitCode = 1)

      val ex = intercept[ProcessFailedException] {
        job.runPythonCmdLine(pramenPyConfigOpt.get, infoDate, conf)
      }

      assert(ex.getMessage == "The process has exited with error code 1.")
    }
  }

  "runPythonOnDatabricks" should {
    "run the proper command line" in {
      val (job, _, _, databricksClientOpt) = getUseCase(useDatabricks = true)
      val databricksClient = databricksClientOpt.get

      job.runPythonOnDatabricks(databricksClient, infoDate, conf)

      assert(databricksClient.createFileInvocations.length == 1)
      assert(databricksClient.runTransientJobInvocations.length == 1)
    }

    "throws RuntimeException if the databricks job fails throws an exception" in {
      val (job, _, _, databricksClientOpt) = getUseCase(
        useDatabricks = true,
        runException = new IllegalStateException("Dummy")
      )
      val databricksClient = databricksClientOpt.get

      val ex = intercept[RuntimeException] {
        job.runPythonOnDatabricks(databricksClient, infoDate, conf)
      }

      assert(ex.getMessage.contains("The Databricks job has failed."))
      assert(ex.getCause.getMessage.contains("Dummy"))
    }
  }

  "getMetastoreConfig" should {
    val expected =
      """run_transformers:
        |- info_date: 2022-02-18
        |  output_table: table2
        |  name: python_class
        |  spark_config: {}
        |  options: {}
        |metastore_tables:
        |- name: table1
        |  description: description
        |  format: parquet
        |  path: /tmp/dummy
        |  records_per_partition: 500000
        |  info_date_settings:
        |    column: INFO_DATE
        |    format: yyyy-MM-dd
        |    start: 2020-01-31
        |  reader_options: {}
        |  writer_options: {}
        |- name: table2
        |  description: description
        |  format: parquet
        |  path: /tmp/dummy
        |  records_per_partition: 500000
        |  info_date_settings:
        |    column: INFO_DATE
        |    format: yyyy-MM-dd
        |    start: 2020-01-31
        |  reader_options: {}
        |  writer_options: {}
        |""".stripMargin

    "creates a temporary config" in {
      val (job, _, _, _) = getUseCase()

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
          |  spark_config: {}
          |  options: {}
          |metastore_tables:
          |- name: table1
          |  description: description
          |  format: parquet
          |  path: /tmp/dummy
          |  records_per_partition: 500000
          |  info_date_settings:
          |    column: INFO_DATE
          |    format: yyyy-MM-dd
          |    start: 2020-01-31
          |  reader_options: {}
          |  writer_options: {}
          |- name: table2
          |  description: description
          |  format: parquet
          |  path: /tmp/dummy
          |  records_per_partition: 500000
          |  info_date_settings:
          |    column: INFO_DATE
          |    format: yyyy-MM-dd
          |    start: 2020-01-31
          |  reader_options: {}
          |  writer_options: {}
          |""".stripMargin

      val (job, _, _, _) = getUseCase()

      val actual = job.getYamlConfig(infoDate)

      compareText(actual, expected)
    }
    "generate yaml config from the metastore and task definition with extra options and Spark config" in {
      val expected =
        """run_transformers:
          |- info_date: 2022-02-18
          |  output_table: table2
          |  name: python_class
          |  spark_config:
          |    spark.driver.host: 127.0.0.1
          |    spark.executor.cores: 1
          |    spark.executor.instances: 1
          |  options:
          |    key.2: "value'2'"
          |    key1: value1
          |metastore_tables:
          |- name: table1
          |  description: description
          |  format: parquet
          |  path: /tmp/dummy
          |  records_per_partition: 500000
          |  info_date_settings:
          |    column: INFO_DATE
          |    format: yyyy-MM-dd
          |    start: 2020-01-31
          |  reader_options: {}
          |  writer_options: {}
          |- name: table2
          |  description: description
          |  format: parquet
          |  path: /tmp/dummy
          |  records_per_partition: 500000
          |  info_date_settings:
          |    column: INFO_DATE
          |    format: yyyy-MM-dd
          |    start: 2020-01-31
          |  reader_options: {}
          |  writer_options: {}
          |""".stripMargin

      val (job, _, _, _) = getUseCase(
        sparkConfig = Map[String, String](
          "spark.driver.host" -> "127.0.0.1",
          "spark.executor.instances" -> "1",
          "spark.executor.cores" -> "1"
        ),
        extraOptions = Map[String, String](
          "key1" -> "value1",
          "key.2" -> "value'2'"
        )
      )

      val actual = job.getYamlConfig(infoDate)

      compareText(actual, expected)
    }

    "generate yaml config from the metastore and task definition with extra reader and writer options for metastore tables" in {
      val expected =
        """run_transformers:
          |- info_date: 2022-02-18
          |  output_table: table2
          |  name: python_class
          |  spark_config: {}
          |  options: {}
          |metastore_tables:
          |- name: table1
          |  description: description
          |  format: parquet
          |  path: /tmp/dummy
          |  records_per_partition: 500000
          |  info_date_settings:
          |    column: INFO_DATE
          |    format: yyyy-MM-dd
          |    start: 2020-01-31
          |  reader_options:
          |    mergeSchema: true
          |  writer_options:
          |    compression: snappy
          |- name: table2
          |  description: description
          |  format: parquet
          |  path: /tmp/dummy
          |  records_per_partition: 500000
          |  info_date_settings:
          |    column: INFO_DATE
          |    format: yyyy-MM-dd
          |    start: 2020-01-31
          |  reader_options:
          |    mergeSchema: true
          |  writer_options:
          |    compression: snappy
          |""".stripMargin

      val (job, _, _, _) = getUseCase(
        readOptions = Map[String, String](
          "mergeSchema" -> "true"
        ),
        writeOptions = Map[String, String](
          "compression" -> "snappy"
        )
      )

      val actual = job.getYamlConfig(infoDate)

      compareText(actual, expected)
    }

    "getTemporaryPathForYamlConfig" should {
      "generate a temporary path for pramen-py config" in {
        val conf = ConfigFactory.parseString(
          """
            |pramen.temporary.directory = "/tmp/pramen/"
            |""".stripMargin)
        val (job, _, _, _) = getUseCase()

        val temporaryDirectory = job.getTemporaryPathForYamlConfig(conf)

        assert(temporaryDirectory.startsWith("dbfs:/tmp/pramen"))
        assert(temporaryDirectory.endsWith("config.yaml"))
      }
    }
  }

  def getUseCase(exitCode: Int = 0,
                 tableDf: DataFrame = null,
                 useCommandLine: Boolean = true,
                 useDatabricks: Boolean = false,
                 runException: Throwable = null,
                 tableException: Throwable = null,
                 stats: MetaTableStats = null,
                 statsException: Throwable = null,
                 sparkConfig: Map[String, String] = Map.empty[String, String],
                 extraOptions: Map[String, String] = Map.empty[String, String],
                 readOptions: Map[String, String] = Map.empty[String, String],
                 writeOptions: Map[String, String] = Map.empty[String, String]): (PythonTransformationJob, ProcessRunnerSpy, Option[PramenPyCmdConfig], Option[DatabricksClientSpy]) = {
    val bk = new SyncBookkeeperMock
    val metastore = new MetastoreSpy(
      tableDf = tableDf,
      tableException = tableException,
      stats = stats,
      statsException = statsException,
      readOptions = readOptions,
      writeOptions = writeOptions
    )
    val operationDef = OperationDefFactory.getDummyOperationDef(sparkConfig = sparkConfig, extraOptions = extraOptions)

    val outputTable = MetaTableFactory.getDummyMetaTable(name = "table2")

    val pramenPyConfigOpt = if (useCommandLine) {
      val cmdConfig = PramenPyCmdConfig("/dummy/path", "dummy_cmd", "@location/@executable transformations run @pythonClass")
      Some(cmdConfig)
    }  else {
      None
    }

    val processRunner = new ProcessRunnerSpy(exitCode, runException = runException)

    val databricksClientOpt = if (useDatabricks) {
      val client = new DatabricksClientSpy(runException)
      Some(client)
    } else {
      None
    }

    val job = new PythonTransformationJob(
      operationDef,
      metastore,
      bk,
      Nil,
      outputTable,
      "python_class",
      pramenPyConfigOpt,
      processRunner,
      databricksClientOpt
    )

    (job, processRunner, pramenPyConfigOpt, databricksClientOpt)
  }
}
