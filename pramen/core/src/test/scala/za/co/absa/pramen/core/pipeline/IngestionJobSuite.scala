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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.spark.sql.DataFrame
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.api.{Query, Reason}
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.config.Keys.SPECIAL_CHARACTERS_IN_COLUMN_NAMES
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.metastore.peristence.TransientTableManager
import za.co.absa.pramen.core.mocks.MetaTableFactory
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.core.pipeline.JobBase._
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.source.SourceManager.getSourceByName
import za.co.absa.pramen.core.utils.SparkUtils

import java.sql.SQLSyntaxErrorException
import java.time.{Instant, LocalDate}

class IngestionJobSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture with RelationalDbFixture with TempDirFixture {
  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  private val runReason: TaskRunReason = TaskRunReason.New

  private val conf: Config = ConfigFactory.parseString(
    s"""
       | pramen {
       |   special.characters.in.column.names = "' :+-=<>()[]{}*?/\\\""
       |   sources = [
       |    {
       |      name = "jdbc"
       |      factory.class = "za.co.absa.pramen.core.source.JdbcSource"
       |      jdbc {
       |        driver = "$driver"
       |        connection.string = "$url"
       |        user = "$user"
       |        password = "$password"
       |      }
       |
       |      has.information.date.column = false
       |    },
       |    {
       |      name = "jdbc_info_date"
       |      factory.class = "za.co.absa.pramen.core.source.JdbcSource"
       |      jdbc {
       |        driver = "$driver"
       |        connection.string = "$url"
       |        user = "$user"
       |        password = "$password"
       |      }
       |
       |      has.information.date.column = true
       |      information.date.column = "info_date"
       |      information.date.type = "string"
       |      information.date.format = "yyyy-MM-dd"
       |    },
       |  ]
       | }
       |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    RdbExampleTable.Company.initTable(getConnection)
    RdbExampleTable.Empty.initTable(getConnection)
  }

  override protected def afterAll(): Unit = {
    RdbExampleTable.Company.dropTable(getConnection)
    RdbExampleTable.Empty.dropTable(getConnection)
    super.afterAll()
  }

  "trackDays" should {
    "return 0 for a snapshot table" in {
      val (_, _, job) = getUseCase()

      assert(job.trackDays == 0)
    }

    "return non-zero for a snapshot table with explicit track days" in {
      val (_, _, job) = getUseCase(trackDaysExplicitlySet = true)

      assert(job.trackDays == 1)
    }

    "return non-zero for an event table" in {
      val (_, _, job) = getUseCase(sourceName = "jdbc_info_date")

      assert(job.trackDays == 1)
    }
  }

  "preRunCheckJob" should {
    "single day inputs" should {
      "track already ran" in {
        val (bk, _, job) = getUseCase()

        bk.setRecordCount("table1", infoDate, infoDate, infoDate, 4, 4, 123, 456, isTableTransient = false)

        val result = job.preRunCheckJob(infoDate, runReason, conf, Nil)

        assert(result.status == JobPreRunStatus.AlreadyRan)
      }

      "track needs update" when {
        "some records, default minimum records" in {
          val (bk, _, job) = getUseCase()

          bk.setRecordCount("table1", infoDate, infoDate, infoDate, 100, 100, 123, 456, isTableTransient = false)

          val result = job.preRunCheckJob(infoDate, runReason, conf, Nil)

          assert(result.status == JobPreRunStatus.NeedsUpdate)
        }

        "some records, custom minimum records" in {
          val (bk, _, job) = getUseCase(minRecords = Some(3))

          bk.setRecordCount("table1", infoDate, infoDate, infoDate, 100, 100, 123, 456, isTableTransient = false)

          val result = job.preRunCheckJob(infoDate, runReason, conf, Nil)

          assert(result.status == JobPreRunStatus.NeedsUpdate)
        }
      }

      "track ready" when {
        "some records, default minimum records" in {
          val (_, _, job) = getUseCase()

          val result = job.preRunCheckJob(infoDate, runReason, conf, Nil)

          assert(result.status == JobPreRunStatus.Ready)
        }

        "some records, custom minimum records" in {
          val (_, _, job) = getUseCase(minRecords = Some(3))

          val result = job.preRunCheckJob(infoDate, runReason, conf, Nil)

          assert(result.status == JobPreRunStatus.Ready)
        }

        "empty table, custom zero minimum records" in {
          val (_, _, job) = getUseCase(minRecords = Some(0), sourceTable = "empty")

          val result = job.preRunCheckJob(infoDate, runReason, conf, Nil)

          assert(result.status == JobPreRunStatus.Ready)
        }
      }

      "track no data" when {
        "no records, default minimum records" in {
          val (_, _, job) = getUseCase(sourceTable = "empty")

          val result = job.preRunCheckJob(infoDate, runReason, conf, Nil)

          assert(result.status == JobPreRunStatus.NoData(false))
        }
      }

      "track insufficient data" when {
        "new job, some records, custom minimum records" in {
          val (_, _, job) = getUseCase(minRecords = Some(5))

          val result = job.preRunCheckJob(infoDate, runReason, conf, Nil)

          assert(result.status == JobPreRunStatus.InsufficientData(4, 5, None))
        }

        "needs update, some records, custom minimum records" in {
          val (bk, _, job) = getUseCase(minRecords = Some(5))

          bk.setRecordCount("table1", infoDate, infoDate, infoDate, 100, 100, 123, 456, isTableTransient = false)

          val result = job.preRunCheckJob(infoDate, runReason, conf, Nil)

          assert(result.status == JobPreRunStatus.InsufficientData(4, 5, Some(100)))
        }
      }
    }

    "ranged inputs" should {
      "already ran" in {
        val (bk, _, job) = getUseCase(sourceName = "jdbc_info_date", rangeFromExpr = Some("@infoDate-1"), rangeToExpr = Some("@infoDate"))

        val noDataInfoDate = infoDate.plusDays(1)

        bk.setRecordCount("table1", noDataInfoDate, noDataInfoDate, noDataInfoDate, 4, 4, 123, 456, isTableTransient = false)

        val result = job.preRunCheckJob(noDataInfoDate, runReason, conf, Nil)

        assert(result.status == JobPreRunStatus.AlreadyRan)
      }

      "track needs update" in {
        val (bk, _, job) = getUseCase(sourceName = "jdbc_info_date", rangeFromExpr = Some("@infoDate-1"), rangeToExpr = Some("@infoDate"))

        val noDataInfoDate = infoDate.plusDays(1)

        bk.setRecordCount("table1", noDataInfoDate, noDataInfoDate, noDataInfoDate, 30, 30, 123, 456, isTableTransient = false)

        val result = job.preRunCheckJob(noDataInfoDate, runReason, conf, Nil)

        assert(result.status == JobPreRunStatus.NeedsUpdate)
      }

      "track ready" in {
        val (bk, _, job) = getUseCase(sourceName = "jdbc_info_date", rangeFromExpr = Some("@infoDate-1"), rangeToExpr = Some("@infoDate"))

        val noDataInfoDate = infoDate.plusDays(1)

        val result = job.preRunCheckJob(noDataInfoDate, runReason, conf, Nil)

        assert(result.status == JobPreRunStatus.Ready)
      }

      "track no data" in {
        val (bk, _, job) = getUseCase(sourceName = "jdbc_info_date", rangeFromExpr = Some("@infoDate-1"), rangeToExpr = Some("@infoDate"))

        val noDataInfoDate = infoDate.plusDays(3)

        val result = job.preRunCheckJob(noDataInfoDate, runReason, conf, Nil)

        assert(result.status == JobPreRunStatus.NoData(false))
      }
    }
  }

  "validate" should {
    "return Ready" in {
      val (_, _, job) = getUseCase(sourceTable = "empty")

      val result = job.validate(infoDate, conf)

      assert(result == Reason.Ready)
    }
  }

  "run" should {
    "get the source data frame" in {
      val (_, _, job) = getUseCase()

      val runResult = job.run(infoDate, conf)

      val df = runResult.data

      assert(df.count() == 4)
      assert(df.schema.fields.head.name == "ID")
      assert(df.schema.fields(1).name == "NAME")
      assert(df.schema.fields(2).name == "DESCRIPTION")
      assert(df.schema.fields(3).name == "EMAIL")
      assert(df.schema.fields(4).name == "FOUNDED")
      assert(df.schema.fields(5).name == "LAST_UPDATED")
    }

    "get the source data frame for source with disabled count query" in {
      withTempDirectory("cached_ingested_data") { tempDir =>

        val (_, _, job) = getUseCase(tempDirectory = Option(tempDir), disableCountQuery = true)

        val preRunCheck = job.preRunCheckJob(infoDate, runReason, conf, Seq.empty)
        assert(preRunCheck.status == JobPreRunStatus.Ready)
        assert(preRunCheck.inputRecordsCount.contains(4))
        assert(TransientTableManager.hasDataForTheDate("source_cache://jdbc|company|2022-02-18", infoDate))

        val runResult = job.run(infoDate, conf)

        val df = runResult.data

        assert(df.count() == 4)
        assert(df.schema.fields.head.name == "ID")
        assert(df.schema.fields(1).name == "NAME")
        assert(df.schema.fields(2).name == "DESCRIPTION")
        assert(df.schema.fields(3).name == "EMAIL")
        assert(df.schema.fields(4).name == "FOUNDED")
        assert(df.schema.fields(5).name == "LAST_UPDATED")

        TransientTableManager.reset()
      }
    }

    "throw an exception when temporary folder is not defined and count query is disabled" in {
      val (_, _, job) = getUseCase(disableCountQuery = true)

      val ex = intercept[IllegalArgumentException] {
        job.run(infoDate, conf)
      }

      assert(ex.getMessage.contains("a temporary directory in Hadoop (HDFS, S3, etc) should be set at 'pramen.temporary.directory'"))
    }

    "throw an exception on read failure" in {
      val (_, _, job) = getUseCase(sourceTable = "noSuchTable")

      val ex = intercept[SQLSyntaxErrorException] {
        job.run(infoDate, conf)
      }

      assert(ex.getMessage.contains("NOSUCHTABLE"))
    }
  }

  "postProcessing" should {
    "apply transformations, filters and projections" in {
      val expectedData =
        """[ {
          |  "ID" : 2,
          |  "NAME" : "Company2",
          |  "NAME_U" : "COMPANY2",
          |  "EMAIL" : "company2@example.com"
          |}, {
          |  "ID" : 3,
          |  "NAME" : "Company3",
          |  "NAME_U" : "COMPANY3",
          |  "EMAIL" : "company3@example.com"
          |}, {
          |  "ID" : 4,
          |  "NAME" : "Company4",
          |  "NAME_U" : "COMPANY4",
          |  "EMAIL" : "company4@example.com"
          |} ]""".stripMargin
      val (_, _, job) = getUseCase()

      val runResult = job.run(infoDate, conf)

      val dfIn = runResult.data

      val dfOut = job.postProcessing(dfIn, infoDate, conf).orderBy("ID")

      val actualData = SparkUtils.dataFrameToJson(dfOut)

      compareText(actualData, expectedData)
    }
  }

  "save" should {
    "save the dataframe to the metastore" in {
      val (_, mt, job) = getUseCase()

      val saveResult = job.save(exampleDf, infoDate, conf, Instant.now(), Some(150))

      val stats = saveResult.stats

      assert(stats.recordCount == 3)
      assert(mt.saveTableInvocations.length == 1)
      assert(mt.saveTableInvocations.head._1 == "table1")
      assert(mt.saveTableInvocations.head._2 == infoDate)
      assert(mt.saveTableInvocations.head._3.schema.treeString == exampleDf.schema.treeString)
    }
  }

  def getUseCase(sourceName: String = "jdbc",
                 sourceTable: String = RdbExampleTable.Company.tableName,
                 rangeFromExpr: Option[String] = None,
                 rangeToExpr: Option[String] = None,
                 minRecords: Option[Int] = None,
                 trackDaysExplicitlySet: Boolean = false,
                 tempDirectory: Option[String] = None,
                 disableCountQuery: Boolean = false): (SyncBookkeeperMock, MetastoreSpy, IngestionJob) = {
    val bk = new SyncBookkeeperMock
    val metastore = new MetastoreSpy
    val operationDef = OperationDefFactory.getDummyOperationDef()

    val configOverride = minRecords.map(min => ConfigFactory.empty().withValue(MINIMUM_RECORDS_KEY, ConfigValueFactory.fromAnyRef(min)))

    val source = getSourceByName(sourceName, conf, configOverride)

    val outputTable = MetaTableFactory.getDummyMetaTable(name = "table1", trackDays = 1, trackDaysExplicitlySet = trackDaysExplicitlySet)

    val specialCharacters = conf.getString(SPECIAL_CHARACTERS_IN_COLUMN_NAMES)

    val tableConf = ConfigFactory.empty()

    val job = new IngestionJob(operationDef,
      metastore,
      bk,
      Nil,
      sourceName,
      source,
      SourceTable("table1", Query.Table(sourceTable), tableConf, rangeFromExpr, rangeToExpr, None, Seq(
        TransformExpression("NAME_U", Some("upper(NAME)"), None)
      ), Seq("ID > 1"), Seq("ID", "NAME", "NAME_U", "EMAIL"), configOverride),
      outputTable,
      specialCharacters,
      tempDirectory,
      disableCountQuery)

    (bk, metastore, job)
  }

}
