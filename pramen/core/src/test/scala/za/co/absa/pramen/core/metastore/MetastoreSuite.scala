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

package za.co.absa.pramen.framework.metastore

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.framework.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.framework.utils.SparkUtils

import java.time.LocalDate

class MetastoreSuite extends WordSpec with SparkTestBase with TextComparisonFixture with TempDirFixture {
  private val infoDate = LocalDate.of(2011, 10, 12)

  "getRegisteredTables()" should {
    "get the list of registered tables" in {
      withTempDirectory("metastore_test") { tempDir =>
        val (m, _) = getTestCase(tempDir)

        val actual = m.getRegisteredTables

        assert(actual.size == 3)
        assert(actual.contains("table1"))
        assert(actual.contains("table2"))
        assert(actual.contains("table3"))
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

  "getReader()" should {
    "return a reader to be able to read the table" in {
      withTempDirectory("metastore_test") { tempDir =>
        val expected =
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

        val reader = m.getReader("table1")

        val dfOpt = reader.getData(infoDate, infoDate)

        assert(dfOpt.isDefined)

        val actual = SparkUtils.dataFrameToJson(dfOpt.get.orderBy("A"))

        compareText(actual, expected)
      }
    }

  }

  "getWriter()" should {
    "return a writer to be able to write to the table" in {
      withTempDirectory("metastore_test") { tempDir =>
        val expected =
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

        val writer = m.getWriter("table1")

        writer.write(getDf.withColumn("p", lit(1)), infoDate, Some(3))

        val df = m.getTable("table1", Some(infoDate), Some(infoDate))

        val actual = SparkUtils.dataFrameToJson(df)

        compareText(actual, expected)
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

  def getDf: DataFrame = {
    import spark.implicits._

    List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
  }

  def getTestCase(tempDir: String, undercover: Boolean = false): (Metastore, SyncBookkeeperMock) = {
    val conf = ConfigFactory.parseString(
      s"""pramen.temporary.directory = "$tempDir/temp"
         |pramen.information.date.column = "sync_date"
         |pramen.information.date.format = "yyyy-MM-dd"
         |pramen.information.date.start = "2011-01-01"
         |pramen.track.days = 4
         |pramen.undercover = $undercover
         |pramen.metastore {
         |  tables = [
         |   {
         |     name = "table1"
         |     format = "parquet"
         |     path = "$tempDir/table1"
         |   },
         |   {
         |     name = "table2"
         |     format = "parquet"
         |     path = "$tempDir/table2"
         |   },
         |   {
         |     name = "table3"
         |     format = "parquet"
         |     path = "$tempDir/table3"
         |   }
         | ]
         |}
         |""".stripMargin
    )

    val bk = new SyncBookkeeperMock
    (MetastoreImpl.fromConfig(conf, bk), bk)
  }
}
