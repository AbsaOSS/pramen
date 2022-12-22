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
import za.co.absa.pramen.api.{Query, Reason}
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.config.Keys.SPECIAL_CHARACTERS_IN_COLUMN_NAMES
import za.co.absa.pramen.core.fixtures.{RelationalDbFixture, TextComparisonFixture}
import za.co.absa.pramen.core.mocks.MetaTableFactory
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.metastore.MetastoreSpy
import za.co.absa.pramen.core.pipeline.JobBase._
import za.co.absa.pramen.core.samples.RdbExampleTable
import za.co.absa.pramen.core.source.SourceManager.getSourceByName
import za.co.absa.pramen.core.utils.SparkUtils

import java.sql.SQLSyntaxErrorException
import java.time.{Instant, LocalDate}

class IngestionJobSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture with RelationalDbFixture {

  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  private val conf: Config = ConfigFactory.parseString(
    s"""
       | pramen {
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
       |      information.date.app.format = "yyyy-MM-dd"
       |      information.date.sql.format = "YYYY-MM-DD"
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

  "preRunCheckJob" should {
    "single day inputs" should {
      "track already ran" in {
        val (bk, _, job) = getUseCase()

        bk.setRecordCount("table1", infoDate, infoDate, infoDate, 3, 3, 123, 456)

        val result = job.preRunCheckJob(infoDate, conf, Nil)

        assert(result.status == JobPreRunStatus.AlreadyRan)
      }

      "track needs update" when {
        "some records, default minimum records" in {
          val (bk, _, job) = getUseCase()

          bk.setRecordCount("table1", infoDate, infoDate, infoDate, 100, 100, 123, 456)

          val result = job.preRunCheckJob(infoDate, conf, Nil)

          assert(result.status == JobPreRunStatus.NeedsUpdate)
        }

        "some records, custom minimum records" in {
          val (bk, _, job) = getUseCase(minRecords = Some(3))

          bk.setRecordCount("table1", infoDate, infoDate, infoDate, 100, 100, 123, 456)

          val result = job.preRunCheckJob(infoDate, conf, Nil)

          assert(result.status == JobPreRunStatus.NeedsUpdate)
        }
      }

      "track ready" when {
        "some records, default minimum records" in {
          val (_, _, job) = getUseCase()

          val result = job.preRunCheckJob(infoDate, conf, Nil)

          assert(result.status == JobPreRunStatus.Ready)
        }

        "some records, custom minimum records" in {
          val (_, _, job) = getUseCase(minRecords = Some(3))

          val result = job.preRunCheckJob(infoDate, conf, Nil)

          assert(result.status == JobPreRunStatus.Ready)
        }

        "empty table, custom zero minimum records" in {
          val (_, _, job) = getUseCase(minRecords = Some(0), sourceTable = "empty")

          val result = job.preRunCheckJob(infoDate, conf, Nil)

          assert(result.status == JobPreRunStatus.Ready)
        }
      }

      "track no data" when {
        "no records, default minimum records" in {
          val (_, _, job) = getUseCase(sourceTable = "empty")

          val result = job.preRunCheckJob(infoDate, conf, Nil)

          assert(result.status == JobPreRunStatus.NoData)
        }
      }

      "track insufficient data" when {
        "new job, some records, custom minimum records" in {
          val (_, _, job) = getUseCase(minRecords = Some(4))

          val result = job.preRunCheckJob(infoDate, conf, Nil)

          assert(result.status == JobPreRunStatus.InsufficientData(3, 4, None))
        }

        "needs update, some records, custom minimum records" in {
          val (bk, _, job) = getUseCase(minRecords = Some(4))

          bk.setRecordCount("table1", infoDate, infoDate, infoDate, 100, 100, 123, 456)

          val result = job.preRunCheckJob(infoDate, conf, Nil)

          assert(result.status == JobPreRunStatus.InsufficientData(3, 4, Some(100)))
        }
      }
    }

    "ranged inputs" should {
      "already ran" in {
        val (bk, _, job) = getUseCase(sourceName = "jdbc_info_date", rangeFromExpr = Some("@infoDate-1"), rangeToExpr = Some("@infoDate"))

        val noDataInfoDate = infoDate.plusDays(1)

        bk.setRecordCount("table1", noDataInfoDate, noDataInfoDate, noDataInfoDate, 3, 3, 123, 456)

        val result = job.preRunCheckJob(noDataInfoDate, conf, Nil)

        assert(result.status == JobPreRunStatus.AlreadyRan)
      }

      "track needs update" in {
        val (bk, _, job) = getUseCase(sourceName = "jdbc_info_date", rangeFromExpr = Some("@infoDate-1"), rangeToExpr = Some("@infoDate"))

        val noDataInfoDate = infoDate.plusDays(1)

        bk.setRecordCount("table1", noDataInfoDate, noDataInfoDate, noDataInfoDate, 30, 30, 123, 456)

        val result = job.preRunCheckJob(noDataInfoDate, conf, Nil)

        assert(result.status == JobPreRunStatus.NeedsUpdate)
      }

      "track ready" in {
        val (bk, _, job) = getUseCase(sourceName = "jdbc_info_date", rangeFromExpr = Some("@infoDate-1"), rangeToExpr = Some("@infoDate"))

        val noDataInfoDate = infoDate.plusDays(1)

        val result = job.preRunCheckJob(noDataInfoDate, conf, Nil)

        assert(result.status == JobPreRunStatus.Ready)
      }

      "track no data" in {
        val (bk, _, job) = getUseCase(sourceName = "jdbc_info_date", rangeFromExpr = Some("@infoDate-1"), rangeToExpr = Some("@infoDate"))

        val noDataInfoDate = infoDate.plusDays(2)

        val result = job.preRunCheckJob(noDataInfoDate, conf, Nil)

        assert(result.status == JobPreRunStatus.NoData)
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

      val df = job.run(infoDate, conf)

      assert(df.count() == 3)
      assert(df.schema.fields.head.name == "ID")
      assert(df.schema.fields(1).name == "NAME")
      assert(df.schema.fields(2).name == "EMAIL")
      assert(df.schema.fields(3).name == "FOUNDED")
      assert(df.schema.fields(4).name == "LAST_UPDATED")
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
          |} ]""".stripMargin
      val (_, _, job) = getUseCase()

      val dfIn = job.run(infoDate, conf)

      val dfOut = job.postProcessing(dfIn, infoDate, conf).orderBy("ID")

      val actualData = SparkUtils.dataFrameToJson(dfOut)

      compareText(actualData, expectedData)
    }
  }

  "save" should {
    "save the dataframe to the metastore" in {
      val (_, mt, job) = getUseCase()

      val stats = job.save(exampleDf, infoDate, conf, Instant.now(), Some(150))

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
                 minRecords: Option[Int] = None): (SyncBookkeeperMock, MetastoreSpy, IngestionJob) = {
    val bk = new SyncBookkeeperMock
    val metastore = new MetastoreSpy
    val operationDef = OperationDefFactory.getDummyOperationDef()

    val configOverride = minRecords.map(min => ConfigFactory.empty().withValue(MINIMUM_RECORDS_KEY, ConfigValueFactory.fromAnyRef(min)))

    val source = getSourceByName(sourceName, conf, configOverride)

    val outputTable = MetaTableFactory.getDummyMetaTable(name = "table1")

    val specialCharacters = conf.getString(SPECIAL_CHARACTERS_IN_COLUMN_NAMES)

    val job = new IngestionJob(operationDef,
      metastore,
      bk,
      source,
      SourceTable("table1", Query.Table(sourceTable), rangeFromExpr, rangeToExpr, Seq(
        TransformExpression("NAME_U", "upper(NAME)")
      ), Seq("ID > 1"), Seq("ID", "NAME", "NAME_U", "EMAIL"), configOverride),
      outputTable,
      specialCharacters)

    (bk, metastore, job)
  }

}
