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

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.WordSpec
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.api.v2.Sink
import za.co.absa.pramen.framework.OperationDefFactory
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.fixtures.TextComparisonFixture
import za.co.absa.pramen.framework.job.{SinkTable, TransformExpression}
import za.co.absa.pramen.framework.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.framework.mocks.job.SinkSpy
import za.co.absa.pramen.framework.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.framework.mocks.{MetaTableFactory, SinkTableFactory}
import za.co.absa.pramen.framework.utils.SparkUtils

import java.time.{Instant, LocalDate}

class SinkJobSuite extends WordSpec with SparkTestBase with TextComparisonFixture {
  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 1, 18)
  private val conf = ConfigFactory.empty()

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "preRunCheckJob" should {
    "return Ready when the input table is available" in {
      val (job, _) = getUseCase(tableDf = exampleDf)

      val actual = job.preRunCheckJob(infoDate, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.Ready, Some(3), Nil))
    }

    "return Ready when the input table has no data" in {
      val (job, _) = getUseCase(tableDf = exampleDf.filter(col("a") > 3))

      val actual = job.preRunCheckJob(infoDate, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.Ready, Some(0), Nil))
    }

    "throw an exception when metastore throws an exception" in {
      val (job, _) = getUseCase(tableException = new RuntimeException("Dummy Exception"))

      val ex = intercept[IllegalStateException] {
        job.preRunCheckJob(infoDate, conf, Nil)
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
  }

  "run" should {
    "returns a dataframe when it is available" in {
      val (job, _) = getUseCase(tableDf = exampleDf)

      val actual = job.run(infoDate, conf)

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
        transformations = Seq(TransformExpression("b1", "cast(b as string)")),
        filters = Seq("b > 1"),
        columns = Seq("a", "b1")
      )

      val (job, _) = getUseCase(sinkTable = sinkTable, tableDf = exampleDf)

      val dfIn = job.run(infoDate, conf)

      val dfOut = job.postProcessing(dfIn, infoDate, conf).orderBy("b1")

      val actualData = SparkUtils.dataFrameToJson(dfOut)

      compareText(actualData, expectedData)
    }

    "throw an exception when preprocessing throws" in {
      val sinkTable = SinkTableFactory.getDummySinkTable(
        transformations = Seq(TransformExpression("b1", "cast(b2 as string)")),
        filters = Seq("b > 1"),
        columns = Seq("a", "b1")
      )

      val (job, _) = getUseCase(sinkTable = sinkTable, tableDf = exampleDf)

      val dfIn = job.run(infoDate, conf)

      val ex = intercept[IllegalStateException] {
        job.postProcessing(dfIn, infoDate, conf).orderBy("b1")
      }

      assert(ex.getMessage == "Preprocessing failed on the sink.")
      assert(ex.getCause.getMessage.contains("cannot resolve '`b2`' given input columns: [a, b]"))
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

    (new SinkJob(operation, metastore, bk, outputTable, sink, sinkTable), bk)
  }

}
