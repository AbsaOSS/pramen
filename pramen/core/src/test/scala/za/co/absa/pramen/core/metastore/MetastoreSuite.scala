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

package za.co.absa.pramen.core.metastore

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{CachePolicy, DataFormat}
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.metadata.MetadataManagerNull
import za.co.absa.pramen.core.metastore.peristence.TransientJobManager
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.job.JobSpy
import za.co.absa.pramen.core.mocks.utils.hive.QueryExecutorMock
import za.co.absa.pramen.core.schedule.Schedule
import za.co.absa.pramen.core.utils.SparkUtils
import za.co.absa.pramen.core.utils.hive.{HiveHelperSql, HiveQueryTemplates, QueryExecutorSpark}

import java.time.LocalDate

class MetastoreSuite extends AnyWordSpec with SparkTestBase with TextComparisonFixture with TempDirFixture {
  private val infoDate = LocalDate.of(2011, 10, 12)

  "getRegisteredTables()" should {
    "get the list of registered tables" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        val actual = m.getRegisteredTables

        assert(actual.size == 7)
        assert(actual.contains("table1"))
        assert(actual.contains("table2"))
        assert(actual.contains("table3"))
        assert(actual.contains("transient_table"))
      }
    }
  }

  "isTableAvailable()" should {
    "return if a table is available for a certain info date" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf)

        assert(m.isTableAvailable("table1", infoDate))
        assert(!m.isTableAvailable("table1", infoDate.plusDays(1)))
        assert(!m.isTableAvailable("table1", infoDate.minusDays(1)))
        assert(!m.isTableAvailable("table2", infoDate))
      }
    }

    "return true if an lazy transient table is available for the info date" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        assert(!m.isTableAvailable("transient_table", infoDate))
        assert(m.isTableAvailable("transient_table", infoDate.minusDays(11))) // First day of the month as per use case
      }
    }
  }

  "isDataAvailable()" should {
    "return true if a table is available for a certain info date period" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf)

        assert(m.isDataAvailable("table1", Some(infoDate), Some(infoDate.plusDays(1))))
        assert(!m.isDataAvailable("table1", Some(infoDate.plusDays(1)), Some(infoDate.plusDays(2))))
        assert(!m.isDataAvailable("table1", Some(infoDate.minusDays(2)), Some(infoDate.minusDays(1))))
        assert(!m.isDataAvailable("table2", Some(infoDate), Some(infoDate.plusDays(1))))
      }
    }

    "return true for an open interval if the table is lazy transient" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        val firstDayOfMonth = infoDate.minusDays(11)

        assert(m.isDataAvailable("transient_table", Some(firstDayOfMonth), Some(firstDayOfMonth)))
        assert(m.isDataAvailable("transient_table", Some(firstDayOfMonth.minusDays(1)), Some(firstDayOfMonth.plusDays(1))))
        assert(!m.isDataAvailable("transient_table", Some(firstDayOfMonth.plusDays(1)), Some(firstDayOfMonth.plusDays(2))))

        // For transient lazy tables half-intervals data availability is always true
        assert(m.isDataAvailable("transient_table", Some(firstDayOfMonth), None))
        assert(m.isDataAvailable("transient_table", None, Some(firstDayOfMonth)))
        assert(m.isDataAvailable("transient_table", None, None))
      }

    }
  }

  "getTable()" should {
    "return the table if it is available" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf)
        m.saveTable("table1", infoDate.plusDays(1), getDf)

        val df1 = m.getTable("table1", Some(infoDate), Some(infoDate))
        val df2 = m.getTable("table1", Some(infoDate), None)
        val df3 = m.getTable("table1", None, Some(infoDate))
        val df4 = m.getTable("table1", None, None)

        assert(df1.count() == 3)
        assert(df2.count() == 6)
        assert(df3.count() == 3)
        assert(df4.count() == 6)
      }
    }

    "return an empty dataframe with the expected schema even when the table is not available" in {
      withTempDirectory("metastore_test") { tempDir =>
        val expectedSchema =
          """root
            | |-- a: string (nullable = true)
            | |-- b: integer (nullable = true)
            | |-- sync_date: date (nullable = true)""".stripMargin

        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate.plusDays(1), getDf)

        val df1 = m.getTable("table1", Some(infoDate), Some(infoDate))

        val actualSchema = df1.schema.treeString

        assert(df1.count() == 0)
        compareText(actualSchema, expectedSchema)
      }
    }

    "throw an exception is the table is not available at all" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        val ex = intercept[AnalysisException] {
          m.getTable("table1", Some(infoDate), Some(infoDate))
        }

        assert(ex.getMessage().contains("Path does not exist"))
      }
    }
  }

  "getLatest()" should {
    "return the latest partition" in {
      withTempDirectory("metastore_test") { tempDir =>
        val expected1 =
          """[ {
            |  "a" : "A",
            |  "b" : 1,
            |  "p" : 2,
            |  "sync_date" : "2011-10-13"
            |}, {
            |  "a" : "B",
            |  "b" : 2,
            |  "p" : 2,
            |  "sync_date" : "2011-10-13"
            |}, {
            |  "a" : "C",
            |  "b" : 3,
            |  "p" : 2,
            |  "sync_date" : "2011-10-13"
            |} ]""".stripMargin

        val expected2 =
          """[ {
            |  "a" : "A",
            |  "b" : 1,
            |  "p" : 1,
            |  "sync_date" : "2011-10-12"
            |}, {
            |  "a" : "B",
            |  "b" : 2,
            |  "p" : 1,
            |  "sync_date" : "2011-10-12"
            |}, {
            |  "a" : "C",
            |  "b" : 3,
            |  "p" : 1,
            |  "sync_date" : "2011-10-12"
            |} ]""".stripMargin

        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf.withColumn("p", lit(1)))
        m.saveTable("table1", infoDate.plusDays(1), getDf.withColumn("p", lit(2)))

        val df1 = m.getLatest("table1", None)
        val df2 = m.getLatest("table1", Some(infoDate))

        val actual1 = SparkUtils.dataFrameToJson(df1.orderBy("A"))
        val actual2 = SparkUtils.dataFrameToJson(df2.orderBy("A"))

        compareText(actual1, expected1)
        compareText(actual2, expected2)
      }
    }

    "throw an exception if the data is not available" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf.withColumn("p", lit(1)))

        val ex = intercept[NoDataInTable] {
          m.getLatest("table1", Some(infoDate.minusDays(1)))
        }

        assert(ex.getMessage.contains("table1"))
      }
    }
  }

  "saveTable()" should {
    "save data to the metastore" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, b) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf)

        val df1 = m.getTable("table1", Some(infoDate), Some(infoDate))

        assert(df1.count() == 3)
        assert(b.getDataChunks("table1", infoDate, infoDate).nonEmpty)
      }
    }

    "do not update bookkeeper in undercover mode" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, b) = getTestCase(tempDir, undercover = true)

        m.saveTable("table1", infoDate, getDf)

        val df1 = m.getTable("table1", Some(infoDate), Some(infoDate))

        assert(df1.count() == 3)
        assert(b.getDataChunks("table1", infoDate, infoDate).isEmpty)
      }
    }
  }

  "getHiveHelper" should {
    "get helper Hive helper based on config" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf)

        val hiveHelper = m.getHiveHelper("table1")

        assert(hiveHelper.isInstanceOf[HiveHelperSql])

        assert(hiveHelper.asInstanceOf[HiveHelperSql].queryExecutor.isInstanceOf[QueryExecutorSpark])
      }
    }
  }

  "repairOrCreateHiveTable()" should {
    withTempDirectory("metastore_test") { tempDir =>
      val (m, _) = getTestCase(tempDir)
      val defaultTemplates = HiveQueryTemplates.getDefaultQueryTemplates

      val schema = StructType.fromDDL("id int, name string")

      "do nothing if hive table is not defined" in {
        val qe = new QueryExecutorMock(tableExists = true)
        val hh = new HiveHelperSql(qe, defaultTemplates, true)

        m.repairOrCreateHiveTable("table1", infoDate, Option(schema), hh, recreate = false)

        assert(qe.queries.isEmpty)
      }

      "repair existing table" in {
        val qe = new QueryExecutorMock(tableExists = true)
        val hh = new HiveHelperSql(qe, defaultTemplates, false)

        m.repairOrCreateHiveTable("table_hive_parquet", infoDate, Option(schema), hh, recreate = false)

        assert(qe.queries.length == 1)
        assert(qe.queries.exists(_.contains("REPAIR")))
      }

      "do nothing for a delta since it does not need repairing" in {
        val qe = new QueryExecutorMock(tableExists = true)
        val hh = new HiveHelperSql(qe, defaultTemplates, true)

        m.repairOrCreateHiveTable("table_hive_delta", infoDate, Option(schema), hh, recreate = false)

        assert(qe.queries.isEmpty)
      }

      "re-create parquet table if not exist" in {
        val qe = new QueryExecutorMock(tableExists = false)
        val hh = new HiveHelperSql(qe, defaultTemplates, false)

        m.repairOrCreateHiveTable("table_hive_parquet", infoDate, Option(schema), hh, recreate = false)

        assert(qe.queries.length == 3)
        assert(qe.queries.exists(_.contains("DROP")))
        assert(qe.queries.exists(_.contains("CREATE")))
        assert(qe.queries.exists(_.contains("/dummy/hive/path")))
        assert(qe.queries.exists(_.contains("REPAIR")))
      }

      "re-create delta table if not exist" in {
        val qe = new QueryExecutorMock(tableExists = false)
        val hh = new HiveHelperSql(qe, defaultTemplates, true)

        m.repairOrCreateHiveTable("table_hive_delta", infoDate, Option(schema), hh, recreate = false)

        assert(qe.queries.length == 3)
        assert(qe.queries.exists(_.contains("DROP")))
        assert(qe.queries.exists(_.contains("CREATE")))
        assert(qe.queries.exists(_.contains("/table_hive_delta'")))
        assert(qe.queries.exists(_.contains("REPAIR")))
      }

      "re-create if requested" in {
        val qe = new QueryExecutorMock(tableExists = true)
        val hh = new HiveHelperSql(qe, defaultTemplates, false)

        m.repairOrCreateHiveTable("table_hive_parquet", infoDate, Option(schema), hh, recreate = true)

        assert(qe.queries.length == 3)
        assert(qe.queries.exists(_.contains("DROP")))
        assert(qe.queries.exists(_.contains("CREATE")))
        assert(qe.queries.exists(_.contains("REPAIR")))
      }

      "throw an exception if query is not supported" in {
        val qe = new QueryExecutorMock(tableExists = true)
        val hh = new HiveHelperSql(qe, defaultTemplates, true)

        val ex = intercept[IllegalArgumentException] {
          m.repairOrCreateHiveTable("table_hive_not_supported", infoDate, Option(schema), hh, recreate = false)
        }

        assert(ex.getMessage.contains("Unsupported query type 'table'"))
      }
    }
  }

  "getStats()" should {
    "return stats of the saved table" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf)

        val stats = m.getStats("table1", infoDate)

        assert(stats.recordCount == 3)
        assert(stats.dataSizeBytes.exists(_ > 0))
      }
    }
  }

  "getMetastoreReader()" should {
    "return a reader that reads configured tables" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf)

        val reader = m.getMetastoreReader("table1" :: Nil, infoDate)

        val df1 = reader.getTable("table1", Some(infoDate), Some(infoDate))

        assert(df1.count() == 3)
      }
    }

    "return a reader that throws an exception if table is not configured" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf)

        val reader = m.getMetastoreReader("table2" :: Nil, infoDate)

        val ex = intercept[TableNotConfigured] {
          reader.getTable("table1", Some(infoDate), Some(infoDate))
        }

        assert(ex.getMessage.contains("table1"))
      }
    }

    "return a reader that returns the run info for the specified table" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf)

        val reader = m.getMetastoreReader("table1" :: Nil, infoDate)
        val runInfo1 = reader.getTableRunInfo("table1", infoDate)
        val runInfo2 = reader.getTableRunInfo("table1", infoDate.plusDays(1))

        assert(runInfo1.isDefined)
        assert(runInfo2.isEmpty)

        assert(runInfo1.get.tableName == "table1")
        assert(runInfo1.get.infoDate == infoDate)
      }
    }

    "return a reader that returns a metadata manager" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        m.saveTable("table1", infoDate, getDf)

        val reader = m.getMetastoreReader("table1" :: Nil, infoDate)
        val metadataManager = reader.metadataManager

        metadataManager.setMetadata("table1", infoDate, "key1", "value1")
        val value = metadataManager.getMetadata("table1", infoDate, "key1")

        assert(value.exists(_.value == "value1"))
      }
    }

    "getLatestAvailableDate()" should {
      "return the latest date for a table" in {
        withTempDirectory("metastore_test") { tempDir =>
          val (m, _) = getTestCase(tempDir)

          m.saveTable("table1", infoDate, getDf)
          m.saveTable("table1", infoDate.plusDays(1), getDf)

          val reader = m.getMetastoreReader("table1" :: "table2" :: Nil, infoDate.plusDays(10))

          val date1 = reader.getLatestAvailableDate("table1")
          val date2 = reader.getLatestAvailableDate("table1", Some(infoDate))
          val date3 = reader.getLatestAvailableDate("table1", Some(infoDate.minusDays(1)))
          val date4 = reader.getLatestAvailableDate("table2", None)

          assert(date1.contains(infoDate.plusDays(1)))
          assert(date2.contains(infoDate))
          assert(date3.isEmpty)
          assert(date4.isEmpty)
        }
      }
    }
  }

  "withSparkConfig()" should {
    "set the config at runtime, and restore the original config afterwards" in {
      val sparkConfig = Map(
        "spark.sql.sources.commitProtocolClass" -> "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
        "spark.sql.parquet.output.committer.class" -> "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
        "spark.pramen.test" -> "test"
      )

      var inner1: String = null
      var inner2: String = null
      var inner3: String = null

      MetastoreImpl.withSparkConfig(sparkConfig) {
        inner1 = spark.conf.get("spark.sql.sources.commitProtocolClass")
        inner2 = spark.conf.get("spark.sql.parquet.output.committer.class")
        inner3 = spark.conf.get("spark.pramen.test")
      }

      val outer1 = spark.conf.get("spark.sql.sources.commitProtocolClass")
      val outer2 = spark.conf.get("spark.sql.parquet.output.committer.class")
      val outer3 = spark.conf.getOption("spark.pramen.test")

      assert(inner1 == "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
      assert(inner2 == "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
      assert(inner3 == "test")
      assert(outer1 != "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
      assert(outer2 != "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
      assert(outer3.isEmpty)
    }
  }

  def getDf: DataFrame = {
    import spark.implicits._

    List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
  }

  def getTestCase(tempDir: String, undercover: Boolean = false): (Metastore, SyncBookkeeperMock) = {
    TransientJobManager.reset()
    val tempDirEscaped = tempDir.replace("\\","\\\\")

    val confString =
      s"""pramen.temporary.directory = "$tempDirEscaped/temp"
         |pramen.information.date.column = "sync_date"
         |pramen.information.date.format = "yyyy-MM-dd"
         |pramen.information.date.start = "2011-01-01"
         |pramen.track.days = 4
         |pramen.undercover = $undercover
         |pramen.metastore {
         |  tables.1 = [
         |   {
         |     name = "table1"
         |     format = "parquet"
         |     path = "$tempDirEscaped/table1"
         |   }
         | ]
         |}
         |pramen.metastore {
         |  tables.2 = [
         |   {
         |     name = "table2"
         |     format = "parquet"
         |     path = "$tempDirEscaped/table2"
         |   },
         |   {
         |     name = "table3"
         |     format = "parquet"
         |     path = "$tempDirEscaped/table3"
         |   },
         |   {
         |     name = "transient_table"
         |     format = "transient"
         |   },
         |   {
         |     name = "table_hive_parquet"
         |     format = "parquet"
         |     path = "$tempDirEscaped/table_hive_parquet"
         |     hive.table = "hive_table_parquet"
         |     hive.path = "/dummy/hive/path"
         |   },
         |   {
         |     name = "table_hive_delta"
         |     format = "delta"
         |     path = "$tempDirEscaped/table_hive_delta"
         |     hive.table = "hive_table_delta"
         |   },
         |   {
         |     name = "table_hive_not_supported"
         |     format = "delta"
         |     table = "not_supported"
         |     hive.table = "hive_table_not_supported"
         |   }
         | ]
         |}
         |""".stripMargin

    val conf = ConfigFactory.parseString(
      confString
    ).withFallback(ConfigFactory.load())

    val schedule = Schedule.Monthly(Seq(1))
    TransientJobManager.addLazyJob(
      new JobSpy(outputTableIn = "transient_table",
        outputTableFormat = DataFormat.Transient(CachePolicy.NoCache),
        operationDef = OperationDefFactory.getDummyOperationDef(schedule = schedule))
    )

    val infoDateConfig = InfoDateConfig.fromConfig(conf)
    val bk = new SyncBookkeeperMock
    val mm = new MetadataManagerNull(isPersistenceEnabled = false)
    (MetastoreImpl.fromConfig(conf, infoDateConfig, bk, mm), bk)
  }
}
