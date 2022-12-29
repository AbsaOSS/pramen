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

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.functions.{concat, lit}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TextComparisonFixture
import za.co.absa.pramen.core.mocks.TransferTableFactory
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.job.{SinkSpy, SourceSpy}
import za.co.absa.pramen.core.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.core.utils.SparkUtils

import java.time.{Instant, LocalDate}

class TransferJobSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture {
  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 1, 18)
  private val conf = ConfigFactory.empty()

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "preRunCheckJob" should {
    "return Ready when the input table is available" in {
      val (job, _) = getUseCase()

      val actual = job.preRunCheckJob(infoDate, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.Ready, Some(5), Nil))
    }

    "return NoData when the input table has no data" in {
      val (job, _) = getUseCase(numberOfRecords = 0)

      val actual = job.preRunCheckJob(infoDate, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.NoData, None, Nil))
    }

    "return NeedsUpdate when the number of records do not match" in {
      val (job, bk) = getUseCase(numberOfRecords = 7)

      bk.setRecordCount("table1->sink", infoDate, infoDate, infoDate, 3, 3, 10000, 1001)

      val actual = job.preRunCheckJob(infoDate, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.NeedsUpdate, Some(7), Nil))
    }

    "return AlreadyRan when the number of records didn't change" in {
      val (job, bk) = getUseCase(numberOfRecords = 7)

      bk.setRecordCount("table1->sink", infoDate, infoDate, infoDate, 7, 3, 10000, 1001)

      val actual = job.preRunCheckJob(infoDate, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.AlreadyRan, Some(7), Nil))
    }

    "throw an exception when metastore throws an exception" in {
      val (job, _) = getUseCase(readerCountException = new RuntimeException("Dummy Exception"))

      val ex = intercept[RuntimeException] {
        job.preRunCheckJob(infoDate, conf, Nil)
      }

      assert(ex.getMessage == "Dummy Exception")
    }
  }

  "validate" should {
    "return Ready when there is some data" in {
      val (job, _) = getUseCase()

      val result = job.validate(infoDate, conf)

      assert(result == Reason.Ready)
    }

    "return Ready when the data frame is empty" in {
      val (job, _) = getUseCase(numberOfRecords = 0)

      val result = job.validate(infoDate, conf)

      assert(result == Reason.Ready)
    }

    "return Ready even when the reader throws" in {
      val (job, _) = getUseCase(readerCountException = new RuntimeException("Dummy Exception"))

      val result = job.validate(infoDate, conf)

      assert(result == Reason.Ready)
    }
  }

  "run" should {
    "returns a dataframe when it is available" in {
      val (job, _) = getUseCase()

      val actual = job.run(infoDate, conf)

      assert(actual.count() == 5)
    }

    "throw an exception when the reader throws an exception" in {
      val (job, _) = getUseCase(readerDataException = new RuntimeException("Dummy Exception"))

      val ex = intercept[RuntimeException] {
        job.run(infoDate, conf)
      }

      assert(ex.getMessage == "Dummy Exception")
    }
  }

  "postProcess" should {
    "apply transformations, filters and projections" in {
      val expectedData =
        """[ {
          |  "a" : "a2",
          |  "b1" : "2"
          |}, {
          |  "a" : "a3",
          |  "b1" : "3"
          |}, {
          |  "a" : "a4",
          |  "b1" : "4"
          |} ]""".stripMargin

      val transferTable = TransferTableFactory.getDummyTransferTable(
        transformations = Seq(
          TransformExpression("b1", "cast(v as string)"),
          TransformExpression("b", "v"),
          TransformExpression("a", "concat('a', b)")
        ),
        filters = Seq("b > 1"),
        columns = Seq("a", "b1")
      )

      val (job, _) = getUseCase(transferTable = transferTable)

      val dfIn = job.run(infoDate, conf)

      val dfOut = job.postProcessing(dfIn, infoDate, conf).orderBy("b1")

      val actualData = SparkUtils.dataFrameToJson(dfOut)

      compareText(actualData, expectedData)
    }

    "throw an exception when preprocessing throws" in {
      val transferTable = TransferTableFactory.getDummyTransferTable(
        transformations = Seq(
          TransformExpression("b1", "cast(x as string)")
        )
      )

      val (job, _) = getUseCase(transferTable = transferTable)

      val dfIn = job.run(infoDate, conf)

      val ex = intercept[AnalysisException] {
        job.postProcessing(dfIn, infoDate, conf)
      }

      assert(ex.getMessage.contains("given input columns: [v]") || ex.getMessage.contains("Column 'x' does not exist"))
    }
  }

  "save" should {
    "update the bookkeeper after the save" in {
      val (job, bk) = getUseCase()

      job.save(exampleDf, infoDate, conf, Instant.now(), Some(10L))

      val outputTable = "table1->sink"

      assert(bk.getDataChunks(outputTable, infoDate, infoDate).nonEmpty)

      val chunk = bk.getDataChunks(outputTable, infoDate, infoDate).head

      assert(chunk.inputRecordCount == 10)
      assert(chunk.outputRecordCount == 3)
    }
  }

  def getUseCase(transferTable: TransferTable = TransferTableFactory.getDummyTransferTable(),
                 numberOfRecords: Int = 5,
                 readerCountException: Throwable = null,
                 readerDataException: Throwable = null): (TransferJob, SyncBookkeeperMock) = {
    val operation = OperationDefFactory.getDummyOperationDef(extraOptions = Map[String, String]("value" -> "7"))

    val bk = new SyncBookkeeperMock

    val metastore = new MetastoreSpy()

    val source = new SourceSpy(numberOfRecords = numberOfRecords,
      readerCountException = readerCountException,
      readerDataException = readerDataException)

    val sink = new SinkSpy()

    val outputTable = transferTable.getMetaTable

    (new TransferJob(operation, metastore, bk, Nil, source, transferTable, outputTable, sink, " "), bk)
  }

}
