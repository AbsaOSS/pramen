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
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.DataFormat
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.databricks.DatabricksClient
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.core.mocks.notify.NotificationTargetSpy
import za.co.absa.pramen.core.mocks.{SinkTableFactory, SourceTableFactory, TransferTableFactory}

class OperationSplitterSuite extends AnyWordSpec with SparkTestBase {
  private val appConfig: Config = ConfigFactory.parseString(
    s""" pramen.metastore.tables = [
       |   {
       |     name = "table1"
       |     format = "delta"
       |     path = "/dummy/path1"
       |     track.days = 2
       |   },
       |   {
       |     name = "table2"
       |     format = "parquet"
       |     path = "/dummy/path2"
       |     track.days = 3
       |   }
       | ]
       |
       | pramen.sources = [
       |   {
       |     name = "spark_source"
       |     factory.class = "za.co.absa.pramen.core.source.SparkSource"
       |
       |     format = "csv"
       |
       |     has.information.date.column = false
       |
       |     option {
       |       header = true
       |       delimiter = ","
       |     }
       |   },
       |   {
       |     name = "spark_optimized_source"
       |     factory.class = "za.co.absa.pramen.core.source.SparkSource"
       |
       |     format = "csv"
       |
       |     has.information.date.column = false
       |
       |     disable.count.query = true
       |
       |     option {
       |       header = true
       |       delimiter = ","
       |     }
       |   }
       | ]
       |
       | pramen.sinks.1 = [
       |  {
       |    name = "spark_sink"
       |    factory.class = "za.co.absa.pramen.core.sink.SparkSink"
       |
       |    format = "parquet"
       |    mode = "overwrite"
       |  }
       |]
       |
       | pramen.notification.targets = [
       |    {
       |      name = "hyperdrive1"
       |      factory.class = "za.co.absa.pramen.core.notify.HyperdriveNotificationTarget"
       |
       |      kafka.topic = "mytopic"
       |
       |      kafka.option {
       |         bootstrap.servers = "dummy:9092,dummy:9093"
       |      }
       |    },
       |    {
       |      name = "custom1"
       |      factory.class = "za.co.absa.pramen.core.mocks.notify.NotificationTargetSpy"
       |
       |      my.config1 = "mykey1"
       |      my.config2 = "mykey2"
       |    },
       |  ]
       |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  private val tableConf: Config = ConfigFactory.parseString(
    s"""notification.target {
       |       my.config2 = "mykey22"
       |       my.config3 = "mykey33"
       |}
       |notification {
       |  token = "AA"
       |}
       |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  "createJobs" should {
    "create ingestion jobs" in {
      val ingestionConf = ConfigFactory.parseString(
        s"""name = "Sourcing operation"
           |source = "spark_source"
           |""".stripMargin)
      val (splitter, _) = getUseCase()

      val sourceTable1 = SourceTableFactory.getDummySourceTable(metaTableName = "table1")
      val sourceTable2 = SourceTableFactory.getDummySourceTable(metaTableName = "table2")

      val ingestionDef = OperationDefFactory.getDummyOperationDef(name = "test",
        operationConf = ingestionConf,
        operationType = OperationType.Ingestion("spark_source", Seq(sourceTable1, sourceTable2))
      )

      val job = splitter.createJobs(ingestionDef)

      assert(job.length == 2)
      assert(job.head.isInstanceOf[IngestionJob])
      assert(job(1).isInstanceOf[IngestionJob])
      assert(job.head.outputTable.trackDays == 2)
    }

    "create ingestion jobs with count query optimization" in {
      val ingestionConf = ConfigFactory.parseString(
        s"""name = "Sourcing operation"
           |source = "spark_optimized_source"
           |""".stripMargin)
      val (splitter, _) = getUseCase()

      val sourceTable1 = SourceTableFactory.getDummySourceTable(metaTableName = "table1")
      val sourceTable2 = SourceTableFactory.getDummySourceTable(metaTableName = "table2")

      val ingestionDef = OperationDefFactory.getDummyOperationDef(name = "test",
        operationConf = ingestionConf,
        operationType = OperationType.Ingestion("spark_optimized_source", Seq(sourceTable1, sourceTable2))
      )

      val job = splitter.createJobs(ingestionDef)

      assert(job.length == 2)
      assert(job.head.isInstanceOf[IngestionJob])
      assert(job(1).isInstanceOf[IngestionJob])
      assert(job.head.trackDays == 0)
    }

    "create transformation jobs" in {
      val transformerConf = ConfigFactory.parseString(
        s"""name = "test transformer"
           |class = "za.co.absa.pramen.core.mocks.transformer.DummyTransformer3"
           |""".stripMargin)
      val (splitter, _) = getUseCase()

      val transformerDef = OperationDefFactory.getDummyOperationDef(name = "test",
        operationConf = transformerConf,
        operationType = OperationType.Transformation("za.co.absa.pramen.core.mocks.transformer.DummyTransformer3", "dummy_output_table")
      )

      val job = splitter.createJobs(transformerDef)

      assert(job.length == 1)
      assert(job.head.isInstanceOf[TransformationJob])
    }

    "create Python transformation jobs" in {
      val transformerConf = ConfigFactory.parseString(
        s"""name = "test Python transformer"
           |python.class = "test_class"
           |""".stripMargin)
      val (splitter, _) = getUseCase()

      val pythonTransformerDef = OperationDefFactory.getDummyOperationDef(name = "test",
        operationConf = transformerConf,
        operationType = OperationType.PythonTransformation("test_class", "dummy_output_table")
      )

      val job = splitter.createJobs(pythonTransformerDef)

      assert(job.length == 1)
      assert(job.head.isInstanceOf[PythonTransformationJob])
    }

    "create sink jobs" in {
      val ingestionConf = ConfigFactory.parseString(
        s"""name = "Sink operation"
           |sink = "spark_sink"
           |""".stripMargin)
      val (splitter, _) = getUseCase()

      val sinkTable1 = SinkTableFactory.getDummySinkTable(metaTableName = "table1")
      val sinkTable2 = SinkTableFactory.getDummySinkTable(metaTableName = "table2")

      val sinkDef = OperationDefFactory.getDummyOperationDef(name = "test",
        operationConf = ingestionConf,
        operationType = OperationType.Sink("spark_sink", Seq(sinkTable1, sinkTable2))
      )

      val job = splitter.createJobs(sinkDef)

      assert(job.length == 2)
      assert(job.head.isInstanceOf[SinkJob])
      assert(job.head.asInstanceOf[SinkJob].outputTable.format.isInstanceOf[DataFormat.Null])
      assert(job(1).isInstanceOf[SinkJob])
      assert(job(1).asInstanceOf[SinkJob].outputTable.format.isInstanceOf[DataFormat.Null])
    }

    "create transfer jobs" in {
      val transferConf = ConfigFactory.parseString(
        s"""name = "Transfer operation"
           |source = "spark_source"
           |sink = "spark_sink"
           |""".stripMargin)
      val (splitter, _) = getUseCase()

      val transferTable1 = TransferTableFactory.getDummyTransferTable()
      val transferTable2 = TransferTableFactory.getDummyTransferTable()

      val transferDef = OperationDefFactory.getDummyOperationDef(name = "test",
        operationConf = transferConf,
        operationType = OperationType.Transfer("spark_source", "spark_sink", Seq(transferTable1, transferTable2))
      )

      val job = splitter.createJobs(transferDef)

      assert(job.length == 2)
      assert(job.head.isInstanceOf[TransferJob])
      assert(job(1).isInstanceOf[TransferJob])
    }
  }

  "createPythonTransformation()" should {
    "create a Python transformation job from operation definition" in {
      val (splitter, operation) = getUseCase()

      val pythonJob = splitter.createPythonTransformation(operation, "HelloWorldTransformation", "output_table1")

      assert(pythonJob.length == 1)
    }
  }

  "getNotificationTargets()" should {
    "get notification targets for a table config" in {
      val jobTarget = OperationSplitter.getNotificationTarget(appConfig, "custom1", tableConf)

      assert(jobTarget.name == "custom1")
      assert(jobTarget.target.isInstanceOf[NotificationTargetSpy])
      assert(jobTarget.target.config.getString("my.config1") == "mykey1")
      assert(jobTarget.target.config.getString("my.config2") == "mykey22")
      assert(jobTarget.target.config.getString("my.config3") == "mykey33")
      assert(jobTarget.options.contains("token"))
      assert(jobTarget.options("token") == "AA")
    }

    "throws an exception if the target does not exist" in {
      assertThrows[IllegalArgumentException] {
        OperationSplitter.getNotificationTarget(appConfig, "does_not_exist", tableConf)
      }
    }
  }

  "getPramenPyCmdlineConfig()" should {
    "create a Pramen-Py command line config" in {
      val conf = ConfigFactory
        .parseString(""" pramen.py.location = "/path/to/pramen-py/venv" """)
        .withFallback(ConfigFactory.load())

      val pramenPyCmdConfig = OperationSplitter.getPramenPyCmdlineConfig(conf)

      assert(pramenPyCmdConfig.isDefined)
      assert(pramenPyCmdConfig.get.isInstanceOf[PramenPyCmdConfig])
    }

    "return None if Pramen-Py location is not defined" in {
      val conf = ConfigFactory.empty()

      val pramenPyCmdConfig = OperationSplitter.getPramenPyCmdlineConfig(conf)

      assert(pramenPyCmdConfig.isEmpty)
    }
  }

  "getDatabricksClient()" should {
    "create a databricks client" in {
      val conf = ConfigFactory.parseString(
        """
          |pramen.py.databricks.host = "some_host"
          |pramen.py.databricks.token = "some_token"
          |""".stripMargin)

      val client = OperationSplitter.getDatabricksClient(conf)

      assert(client.isDefined)
      assert(client.get.isInstanceOf[DatabricksClient])
    }

    "return None if databricks options are not defined" in {
      val conf = ConfigFactory.empty()

      val client = OperationSplitter.getDatabricksClient(conf)

      assert(client.isEmpty)
    }
  }

  def getUseCase(conf: Config = appConfig): (OperationSplitter, OperationDef) = {
    val splitter = new OperationSplitter(conf, new MetastoreSpy(trackDays = 2), new SyncBookkeeperMock)
    val operation = OperationDefFactory.getDummyOperationDef()

    (splitter, operation)
  }
}
