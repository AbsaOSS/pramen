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
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.metastore.peristence.TransientTableManager
import za.co.absa.pramen.core.mocks.TransferTableFactory
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.job.{SinkSpy, SourceSpy}
import za.co.absa.pramen.core.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.core.utils.SparkUtils

import java.time.{Instant, LocalDate}

class TransferJobSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture with TempDirFixture {
  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 1, 18)
  private val conf = ConfigFactory.empty()
  private val runReason: TaskRunReason = TaskRunReason.New

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "preRunCheckJob" should {
    "return Ready when the input table is available" in {
      val (job, _) = getUseCase()

      val actual = job.preRunCheckJob(infoDate, runReason, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.Ready, Some(5), Nil, Nil))
    }

    "return NoData when the input table has no data" in {
      val (job, _) = getUseCase(numberOfRecords = 0)

      val actual = job.preRunCheckJob(infoDate, runReason, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.NoData(false), None, Nil, Nil))
    }

    "return NoData with a failure when the input table has no data" in {
      val (job, _) = getUseCase(numberOfRecords = 0, failOnNoData = true)

      val actual = job.preRunCheckJob(infoDate, runReason, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.NoData(true), None, Nil, Nil))
    }

    "return NeedsUpdate when" +
      " the number of records do not match" in {
      val (job, bk) = getUseCase(numberOfRecords = 7)

      bk.setRecordCount("table1->sink", infoDate, infoDate, infoDate, 3, 3, 10000, 1001, isTableTransient = false)

      val actual = job.preRunCheckJob(infoDate, runReason, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.NeedsUpdate, Some(7), Nil, Nil))
    }

    "return AlreadyRan when the number of records didn't change" in {
      val (job, bk) = getUseCase(numberOfRecords = 7)

      bk.setRecordCount("table1->sink", infoDate, infoDate, infoDate, 7, 3, 10000, 1001, isTableTransient = false)

      val actual = job.preRunCheckJob(infoDate, runReason, conf, Nil)

      assert(actual == JobPreRunResult(JobPreRunStatus.AlreadyRan, Some(7), Nil, Nil))
    }

    "throw an exception when metastore throws an exception" in {
      val (job, _) = getUseCase(getCountException = new RuntimeException("Dummy Exception"))

      val ex = intercept[RuntimeException] {
        job.preRunCheckJob(infoDate, runReason, conf, Nil)
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
      val (job, _) = getUseCase(getDataException = new RuntimeException("Dummy Exception"))

      val result = job.validate(infoDate, conf)

      assert(result == Reason.Ready)
    }
  }

  "run" should {
    "returns a dataframe when it is available" in {
      val (job, _) = getUseCase()

      val actual = job.run(infoDate, conf).data

      assert(actual.count() == 5)
    }

    "get the source data frame for source with disabled count query" in {
      withTempDirectory("cached_transfer_data") { tempDir =>
        val (job, _) = getUseCase(tempDirectory = Option(tempDir), disableCountQuery = true)

        val preRunCheck = job.preRunCheckJob(infoDate, runReason, conf, Seq.empty)
        assert(preRunCheck.status == JobPreRunStatus.Ready)
        assert(preRunCheck.inputRecordsCount.contains(5))
        assert(TransientTableManager.hasDataForTheDate("source_cache://testsource|table1|2022-01-18", infoDate))

        val runResult = job.run(infoDate, conf)

        val df = runResult.data

        assert(df.count() == 5)
        assert(df.schema.fields.head.name == "v")

        TransientTableManager.reset()
      }
    }

    "throw an exception when temporary folder is not defined and count query is disabled" in {
      val (job, _) = getUseCase(disableCountQuery = true)

      val ex = intercept[IllegalArgumentException] {
        job.run(infoDate, conf)
      }

      assert(ex.getMessage.contains("a temporary directory in Hadoop (HDFS, S3, etc) should be set at 'pramen.temporary.directory'"))
    }

    "throw an exception when the reader throws an exception" in {
      val (job, _) = getUseCase(getDataException = new RuntimeException("Dummy Exception"))

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
          TransformExpression("b1", Some("cast(v as string)"), None),
          TransformExpression("b", Some("v"), None),
          TransformExpression("a", Some("concat('a', b)"), None)
        ),
        filters = Seq("b > 1"),
        columns = Seq("a", "b1")
      )

      val (job, _) = getUseCase(transferTable = transferTable)

      val dfIn = job.run(infoDate, conf).data

      val dfOut = job.postProcessing(dfIn, infoDate, conf).orderBy("b1")

      val actualData = SparkUtils.dataFrameToJson(dfOut)

      compareText(actualData, expectedData)
    }

    "throw an exception when preprocessing throws" in {
      val transferTable = TransferTableFactory.getDummyTransferTable(
        transformations = Seq(
          TransformExpression("b1", Some("cast(x as string)"), None)
        )
      )

      val (job, _) = getUseCase(transferTable = transferTable)

      val dfIn = job.run(infoDate, conf).data

      assertThrows[AnalysisException] {
        job.postProcessing(dfIn, infoDate, conf)
      }
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
                 getCountException: Throwable = null,
                 getDataException: Throwable = null,
                 failOnNoData: Boolean = false,
                 tempDirectory: Option[String] = None,
                 disableCountQuery: Boolean = false): (TransferJob, SyncBookkeeperMock) = {
    val operation = OperationDefFactory.getDummyOperationDef(extraOptions = Map[String, String]("value" -> "7"))

    val bk = new SyncBookkeeperMock

    val metastore = new MetastoreSpy()

    val sourceConfig = if (failOnNoData) {
      ConfigFactory.parseString("fail.if.no.data = true")
    } else {
      ConfigFactory.empty()
    }

    val source = new SourceSpy(numberOfRecords = numberOfRecords,
      getCountException = getCountException,
      getDataException = getDataException,
      sourceConfig = sourceConfig)

    val sink = new SinkSpy()

    val outputTable = transferTable.getMetaTable

    (new TransferJob(operation, metastore, bk, Nil, "testSource", source, transferTable, outputTable, sink, " ", tempDirectory, disableCountQuery), bk)
  }

}
