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

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.api.{Reason, Sink}
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TextComparisonFixture
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.job.SinkSpy
import za.co.absa.pramen.core.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.core.mocks.{MetaTableFactory, SinkTableFactory}
import za.co.absa.pramen.core.pipeline.JobBase.MINIMUM_RECORDS_KEY
import za.co.absa.pramen.core.utils.SparkUtils

import java.time.{Instant, LocalDate}

class SinkJobSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture {
  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 1, 18)
  private val conf = ConfigFactory.empty()
  private val runReason: TaskRunReason = TaskRunReason.New

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "preRunCheckJob" should {
    "return Ready when the input table is available" in {
      val (job, _) = getUseCase(tableDf = exampleDf)

      val actual = job.preRunCheckJob(infoDate, runReason, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.Ready, Some(3), Nil, Nil))
    }

    "return Ready when the input table has no data" in {
      val (job, _) = getUseCase(tableDf = exampleDf.filter(col("a") > 3))

      val actual = job.preRunCheckJob(infoDate, runReason, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.Ready, Some(0), Nil, Nil))
    }

    "throw an exception when metastore throws an exception" in {
      val (job, _) = getUseCase(tableException = new RuntimeException("Dummy Exception"))

      val ex = intercept[IllegalStateException] {
        job.preRunCheckJob(infoDate, runReason, conf, Nil)
      }

      assert(ex.getMessage == "Unable to read input table table1 for 2022-01-18.")
      assert(ex.getCause.getMessage == "Dummy Exception")
    }
  }

  "validate" should {
    "return Ready when the data frame is not empty" in {
      val (job, _) = getUseCase(tableDf = exampleDf)

      val result = job.validate(infoDate, conf)

      assert(result == Reason.Ready)
    }

    "return Skip when the data frame is empty" in {
      val (job, _) = getUseCase(tableDf = exampleDf.filter(col("b") > 10))

      val result = job.validate(infoDate, conf)

      assert(result.isInstanceOf[Reason.Skip])
    }

    "return Ready when the data frame satisfies the minimum record count" in {
      val sinkConf = conf.withValue(MINIMUM_RECORDS_KEY, ConfigValueFactory.fromAnyRef(3))
      val sink = SinkSpy(sinkConf, "", spark)

      val (job, _) = getUseCase(sink = sink, tableDf = exampleDf)

      val result = job.validate(infoDate, conf)

      assert(result == Reason.Ready)
    }

    "return NotReady when the data frame does not contain enough records" in {
      val sinkConf = conf.withValue(MINIMUM_RECORDS_KEY, ConfigValueFactory.fromAnyRef(4))
      val sink = SinkSpy(sinkConf, "", spark)

      val (job, _) = getUseCase(sink = sink, tableDf = exampleDf)

      val result = job.validate(infoDate, conf)

      assert(result.isInstanceOf[Reason.NotReady])
    }
  }

  "run" should {
    "returns a dataframe when it is available" in {
      val (job, _) = getUseCase(tableDf = exampleDf)

      val actual = job.run(infoDate, conf).data

      assert(actual.count() == 3)
    }

    "throw an exception when metastore throws an exception" in {
      val (job, _) = getUseCase(tableException = new RuntimeException("Dummy Exception"))

      val ex = intercept[IllegalStateException] {
        job.run(infoDate, conf)
      }

      assert(ex.getMessage == "Unable to read input table table1 for 2022-01-18.")
      assert(ex.getCause.getMessage == "Dummy Exception")
    }
  }

  "postProcess" should {
    "apply transformations, filters and projections" in {
      val expectedData =
        """[ {
          |  "a" : "B",
          |  "b1" : "2"
          |}, {
          |  "a" : "C",
          |  "b1" : "3"
          |} ]""".stripMargin

      val sinkTable = SinkTableFactory.getDummySinkTable(
        transformations = Seq(TransformExpression("b1", Some("cast(b as string)"), None)),
        filters = Seq("b > 1"),
        columns = Seq("a", "b1")
      )

      val (job, _) = getUseCase(sinkTable = sinkTable, tableDf = exampleDf)

      val dfIn = job.run(infoDate, conf).data

      val dfOut = job.postProcessing(dfIn, infoDate, conf).orderBy("b1")

      val actualData = SparkUtils.dataFrameToJson(dfOut)

      compareText(actualData, expectedData)
    }

    "throw an exception when preprocessing throws" in {
      val sinkTable = SinkTableFactory.getDummySinkTable(
        transformations = Seq(TransformExpression("b1", Some("cast(b2 as string)"), None)),
        filters = Seq("b > 1"),
        columns = Seq("a", "b1")
      )

      val (job, _) = getUseCase(sinkTable = sinkTable, tableDf = exampleDf)

      val dfIn = job.run(infoDate, conf).data

      val ex = intercept[IllegalStateException] {
        job.postProcessing(dfIn, infoDate, conf).orderBy("b1")
      }

      assert(ex.getMessage == "Preprocessing failed on the sink.")
      assert(ex.getCause.isInstanceOf[AnalysisException])
    }
  }

  "save" should {
    "invoke connect(), save(), close() methods of the sink and update the bookkeeper" in {
      val df = exampleDf
      val sink = SinkSpy(conf, "", spark)

      val (job, bk) = getUseCase(tableDf = df, sink = sink)

      job.save(df, infoDate, conf, Instant.now(), Some(10L))

      val outputTable = "table1->mysink"

      assert(bk.getDataChunks(outputTable, infoDate, infoDate).nonEmpty)

      val chunk = bk.getDataChunks(outputTable, infoDate, infoDate).head

      assert(chunk.inputRecordCount == 10)
      assert(chunk.outputRecordCount == 3)

      assert(sink.connectCalled == 1)
      assert(sink.writeCalled == 1)
      assert(sink.closeCalled == 1)
    }

    "throw an exception when connect() throws" in {
      val sink = new SinkSpy(connectException = new RuntimeException("Dummy Exception"))

      val (job, _) = getUseCase(tableDf = exampleDf, sink = sink)

      val ex = intercept[IllegalStateException] {
        job.save(exampleDf, infoDate, conf, Instant.now(), Some(10L))
      }

      assert(sink.connectCalled == 1)
      assert(sink.writeCalled == 0)
      assert(sink.closeCalled == 0)
      assert(ex.getMessage == "Unable to connect to the sink.")
      assert(ex.getCause.getMessage == "Dummy Exception")
    }

    "throw an exception when write() throws" in {
      val sink = new SinkSpy(writeException = new RuntimeException("Dummy Exception"))

      val (job, _) = getUseCase(tableDf = exampleDf, sink = sink)

      val ex = intercept[IllegalStateException] {
        job.save(exampleDf, infoDate, conf, Instant.now(), Some(10L))
      }

      assert(sink.connectCalled == 1)
      assert(sink.writeCalled == 1)
      assert(sink.closeCalled == 1)
      assert(ex.getMessage == "Unable to write to the sink.")
      assert(ex.getCause.getMessage == "Dummy Exception")
    }

    "ignore if close() throws" in {
      val sink = new SinkSpy(closeException = new RuntimeException("Dummy Exception"))

      val (job, _) = getUseCase(tableDf = exampleDf, sink = sink)

      job.save(exampleDf, infoDate, conf, Instant.now(), Some(10L))

      assert(sink.connectCalled == 1)
      assert(sink.writeCalled == 1)
      assert(sink.closeCalled == 1)
    }
  }

  def getUseCase(sinkTable: SinkTable = SinkTableFactory.getDummySinkTable(),
                 tableDf: DataFrame = null,
                 tableException: Throwable = null,
                 sink: Sink = SinkSpy(conf, "", spark)): (SinkJob, SyncBookkeeperMock) = {
    val operation = OperationDefFactory.getDummyOperationDef(extraOptions = Map[String, String]("value" -> "7"))

    val bk = new SyncBookkeeperMock

    val metastore = new MetastoreSpy(tableDf = tableDf, tableException = tableException)

    val outputTable = MetaTableFactory.getDummyMetaTable(name = "table1->mysink")

    (new SinkJob(operation, metastore, bk, Nil, outputTable, sink, sinkTable), bk)
  }

}
