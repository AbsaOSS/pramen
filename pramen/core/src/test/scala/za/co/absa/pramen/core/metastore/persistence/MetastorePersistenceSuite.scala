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

package za.co.absa.pramen.core.metastore.persistence

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{CachePolicy, DataFormat, Query}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.metastore.peristence.{MetastorePersistence, MetastorePersistenceDelta, MetastorePersistenceParquet, MetastorePersistenceTransientEager}
import za.co.absa.pramen.core.mocks.MetaTableFactory
import za.co.absa.pramen.core.utils.{LocalFsUtils, SparkUtils}

import java.nio.file.Paths
import java.time.LocalDate

class MetastorePersistenceSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  private val infoDateColumn = "info_date"
  private val infoDateFormat = "yyyy-MM-dd"
  private val infoDate = LocalDate.of(2021, 10, 12)

  def testLoadExistingTable(mtp: MetastorePersistence): Assertion = {
    val expected =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "info_date" : "2021-10-12"
        |} ]""".stripMargin
    mtp.saveTable(infoDate, getDf, Some(3))

    val df = mtp.loadTable(Some(infoDate), Some(infoDate))

    val actual = SparkUtils.dataFrameToJson(df.orderBy("a"))

    compareText(actual, expected)
  }

  def testLoadTablePeriods(mtp: MetastorePersistence): Assertion = {
    val expected1 =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |} ]""".stripMargin
    val expected2 =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "p" : 2,
        |  "info_date" : "2021-10-13"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "p" : 2,
        |  "info_date" : "2021-10-13"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "p" : 2,
        |  "info_date" : "2021-10-13"
        |} ]""".stripMargin
    val expected3 =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |} ]""".stripMargin
    val expected4 =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "A",
        |  "b" : 1,
        |  "p" : 2,
        |  "info_date" : "2021-10-13"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "p" : 2,
        |  "info_date" : "2021-10-13"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "p" : 2,
        |  "info_date" : "2021-10-13"
        |} ]""".stripMargin

    mtp.saveTable(infoDate, getDf.withColumn("p", lit(1)), None)
    mtp.saveTable(infoDate.plusDays(1), getDf.withColumn("p", lit(2)), None)

    val df1 = mtp.loadTable(Some(infoDate), Some(infoDate))
    val df2 = mtp.loadTable(Some(infoDate.plusDays(1)), None)
    val df3 = mtp.loadTable(None, Some(infoDate))
    val df4 = mtp.loadTable(None, None)

    val actual1 = SparkUtils.dataFrameToJson(df1.orderBy("a"))
    val actual2 = SparkUtils.dataFrameToJson(df2.orderBy("a"))
    val actual3 = SparkUtils.dataFrameToJson(df3.orderBy("a"))
    val actual4 = SparkUtils.dataFrameToJson(df4.orderBy("a", "p"))

    compareText(actual1, expected1)
    compareText(actual2, expected2)
    compareText(actual3, expected3)
    compareText(actual4, expected4)
  }

  def testLoadEmptyTable(mtp: MetastorePersistence): Assertion = {
    mtp.saveTable(infoDate, getDf, None)

    val df = mtp.loadTable(Some(infoDate.plusDays(1)), Some(infoDate.plusDays(1)))

    assert(df.count() == 0)
    assert(df.schema.exists(f => f.name == "a"))
    assert(df.schema.exists(f => f.name == "b"))
    assert(df.schema.exists(f => f.name == "info_date"))
  }

  def testNoData(mtp: MetastorePersistence): Assertion = {
    val ex = intercept[AnalysisException] {
      mtp.loadTable(Some(infoDate), Some(infoDate))
    }

    assert(ex.getMessage.contains("exist") || ex.getMessage.contains("is not a Delta table"))
  }

  def testInfoDateExists(mtp: MetastorePersistence): Assertion = {
    val expected =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |} ]""".stripMargin
    mtp.saveTable(infoDate, getDf
      .withColumn("info_date", lit("2021-10-11"))
      .withColumn("p", lit(1)), None)

    val df = mtp.loadTable(Some(infoDate), Some(infoDate)).select("a", "b", "p", "info_date")

    val actual = SparkUtils.dataFrameToJson(df.orderBy("a"))

    compareText(actual, expected)
  }

  def testStatsAvailable(mtp: MetastorePersistence): Assertion = {
    mtp.saveTable(infoDate, getDf, Some(4))

    val stats = mtp.getStats(infoDate)

    assert(stats.recordCount == 3)
    assert(stats.dataSizeBytes.exists(_ > 0))
  }

  def testStatsAvailableForEmptyTable(mtp: MetastorePersistence): Assertion = {
    mtp.saveTable(infoDate, getDf.filter(col("b") > 10), Some(0))

    val stats = mtp.getStats(infoDate)

    assert(stats.recordCount == 0)
    assert(stats.dataSizeBytes.contains(0L))
  }

  def testStatsNotAvailable(mtp: MetastorePersistence): Assertion = {
    val ex = intercept[AnalysisException] {
      mtp.getStats(infoDate)
    }

    assert(ex.getMessage.contains("Path does not exist") ||
      ex.getMessage().contains("doesn't exist") ||
      ex.getMessage().contains("is not a Delta table"))
  }

  def testOverwritePartition(mtp: MetastorePersistence): Assertion = {
    val expected =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "p" : 2,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "p" : 2,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "p" : 2,
        |  "info_date" : "2021-10-12"
        |} ]""".stripMargin
    mtp.saveTable(infoDate, getDf.withColumn("p", lit(1)), None)
    mtp.saveTable(infoDate, getDf.withColumn("p", lit(2)), None)

    val df = mtp.loadTable(Some(infoDate), Some(infoDate))

    val actual = SparkUtils.dataFrameToJson(df.orderBy("a"))

    compareText(actual, expected)
  }

  def testAppendPartition(mtp: MetastorePersistence): Assertion = {
    val expected =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "A",
        |  "b" : 1,
        |  "p" : 2,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "p" : 2,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "p" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "p" : 2,
        |  "info_date" : "2021-10-12"
        |} ]""".stripMargin
    mtp.saveTable(infoDate, getDf.withColumn("p", lit(1)), None)
    mtp.saveTable(infoDate, getDf.withColumn("p", lit(2)), None)

    val df = mtp.loadTable(Some(infoDate), Some(infoDate))

    val actual = SparkUtils.dataFrameToJson(df.orderBy("a", "p"))

    compareText(actual, expected)
  }

  def testSchemaMerge(mtp: MetastorePersistence): Assertion = {
    val expected =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "q" : "2",
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "q" : "2",
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "q" : "2",
        |  "info_date" : "2021-10-12"
        |} ]""".stripMargin
    mtp.saveTable(infoDate, getDf.withColumn("p", lit(1)), None)
    mtp.saveTable(infoDate, getDf.withColumn("q", lit("2")), None)

    val df = mtp.loadTable(Some(infoDate), Some(infoDate))

    val actual = SparkUtils.dataFrameToJson(df.select("a", "b", "q", "info_date").orderBy("a"))

    compareText(actual, expected)
  }

  def testRecordsPerPartition(tempDir: String, mask: String, mtp: MetastorePersistence): Assertion = {
    mtp.saveTable(infoDate, getDf, None)

    val files = LocalFsUtils.getListOfFiles(Paths.get(tempDir), mask)

    assert(files.size == 2)
  }

  def testPathCreation(mtp: MetastorePersistence): Assertion = {
    val expected =
      """[ {
        |  "a" : "A",
        |  "b" : 1,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "B",
        |  "b" : 2,
        |  "info_date" : "2021-10-12"
        |}, {
        |  "a" : "C",
        |  "b" : 3,
        |  "info_date" : "2021-10-12"
        |} ]""".stripMargin
    mtp.saveTable(infoDate, getDf, Some(3))

    val df = mtp.loadTable(Some(infoDate), Some(infoDate))

    val actual = SparkUtils.dataFrameToJson(df.orderBy("a"))

    compareText(actual, expected)
  }

  "apply()" should {
    "create a persistence from config" in {
      withTempDirectory("mt_persist") { tempDir =>
        val mtp1 = getParquetMtPersistence(tempDir)
        val mtp2 = getDeltaMtPersistence(tempDir)

        assert(mtp1.isInstanceOf[MetastorePersistenceParquet])
        assert(mtp2.isInstanceOf[MetastorePersistenceDelta])
      }
    }
  }

  "test metastore persistence in Parquet" when {
    "loadTable()" should {
      "load table if it exists" in {
        withTempDirectory("mt_persist") { tempDir =>
          testLoadExistingTable(getParquetMtPersistence(tempDir))
        }
      }
      "load table periods" in {
        withTempDirectory("mt_persist") { tempDir =>
          testLoadTablePeriods(getParquetMtPersistence(tempDir))
        }
      }
      "load empty table if wrong period" in {
        withTempDirectory("mt_persist") { tempDir =>
          testLoadEmptyTable(getParquetMtPersistence(tempDir))
        }
      }
      "throw an exception is the folder does not exist" in {
        withTempDirectory("mt_persist") { tempDir =>
          testNoData(getParquetMtPersistence(tempDir))
        }
      }
    }

    "saveTable()" should {
      "support fixing the existing info date column" in {
        withTempDirectory("mt_persist") { tempDir =>
          testInfoDateExists(getParquetMtPersistence(tempDir))
        }
      }

      "support partition overwrites" in {
        withTempDirectory("mt_persist") { tempDir =>
          testOverwritePartition(getParquetMtPersistence(tempDir))
        }
      }

      "support partition appends" in {
        withTempDirectory("mt_persist") { tempDir =>
          testAppendPartition(getParquetMtPersistence(tempDir, saveModeOpt = Some(SaveMode.Append)))
        }
      }

      "support schema overwrites" in {
        withTempDirectory("mt_persist") { tempDir =>
          testSchemaMerge(getParquetMtPersistence(tempDir))
        }
      }

      "support records per partition" in {
        withTempDirectory("mt_persist") { tempDir =>
          testRecordsPerPartition(s"$tempDir/parquet/info_date=2021-10-12", "*.parquet", getParquetMtPersistence(tempDir, recordsPerPartition = Some(2)))
        }
      }

      "support path creation" in {
        withTempDirectory("mt_persist") { tempDir =>
          testPathCreation(getParquetMtPersistence(tempDir, pathSuffix = "a/b/c/d"))
        }
      }

      "getStats()" should {
        "get stats after a save" in {
          withTempDirectory("mt_persist") { tempDir =>
            testStatsAvailable(getParquetMtPersistence(tempDir))
          }
        }

        "throw an exception if stats are not available" in {
          withTempDirectory("mt_persist") { tempDir =>
            testStatsNotAvailable(getParquetMtPersistence(tempDir))
          }
        }
      }
    }
  }

  "test metastore persistence in Delta path" when {
    "loadTable()" should {
      "load table if it exists" in {
        withTempDirectory("mt_persist") { tempDir =>
          testLoadExistingTable(getDeltaMtPersistence(tempDir))
        }
      }
      "load table periods" in {
        withTempDirectory("mt_persist") { tempDir =>
          testLoadTablePeriods(getDeltaMtPersistence(tempDir))
        }
      }
      "load empty table if wrong period" in {
        withTempDirectory("mt_persist") { tempDir =>
          testLoadEmptyTable(getDeltaMtPersistence(tempDir))
        }
      }
      "throw an exception is the folder does not exist" in {
        withTempDirectory("mt_persist") { tempDir =>
          testNoData(getDeltaMtPersistence(tempDir))
        }
      }
    }

    "saveTable()" should {
      "supports fixing the existing info date column" in {
        withTempDirectory("mt_persist") { tempDir =>
          testInfoDateExists(getDeltaMtPersistence(tempDir))
        }
      }

      "support partition overwrites" in {
        withTempDirectory("mt_persist") { tempDir =>
          testOverwritePartition(getDeltaMtPersistence(tempDir))
        }
      }

      "support partition appends" in {
        withTempDirectory("mt_persist") { tempDir =>
          testAppendPartition(getDeltaMtPersistence(tempDir, saveModeOpt = Some(SaveMode.Append)))
        }
      }

      "fail of schema change" in {
        withTempDirectory("mt_persist") { tempDir =>
          val ex = intercept[AnalysisException] {
            val mtp = getDeltaMtPersistence(tempDir)
            mtp.saveTable(infoDate, getDf.withColumn("p", lit(1)), None)
            mtp.saveTable(infoDate, getDf.withColumn("p", lit("2")), None)
          }

          assert(ex.getMessage.contains("Failed to merge fields 'p' and 'p'"))
        }
      }

      "fail of schema merge" in {
        withTempDirectory("mt_persist") { tempDir =>
          val ex = intercept[AnalysisException] {
            val mtp = getDeltaMtPersistence(tempDir, writeOptions = Map[String, String]("mergeSchema" -> "false"))
            mtp.saveTable(infoDate, getDf.withColumn("p", lit(1)), None)
            mtp.saveTable(infoDate, getDf.withColumn("q", lit("2")), None)
          }

          assert(ex.getMessage.contains("A schema mismatch detected when writing to the Delta table"))
        }
      }

      "supports schema merges" in {
        withTempDirectory("mt_persist") { tempDir =>
          testSchemaMerge(getDeltaMtPersistence(tempDir))
        }
      }

      "supports records per partition" in {
        withTempDirectory("mt_persist") { tempDir =>
          testRecordsPerPartition(s"$tempDir/delta/info_date=2021-10-12", "*.parquet", getDeltaMtPersistence(tempDir, recordsPerPartition = Some(2)))
        }
      }

      "supports path creation" in {
        withTempDirectory("mt_persist") { tempDir =>
          testPathCreation(getDeltaMtPersistence(tempDir, pathSuffix = "a/b/c/d"))
        }
      }
    }

    "getStats()" should {
      "get stats after a save" in {
        withTempDirectory("mt_persist") { tempDir =>
          testStatsAvailable(getDeltaMtPersistence(tempDir))
        }
      }

      "throw an exception if stats are not available" in {
        withTempDirectory("mt_persist") { tempDir =>
          testStatsNotAvailable(getDeltaMtPersistence(tempDir))
        }
      }
    }

    "transient tables support" in {
      val conf = ConfigFactory.parseString(
        """pramen.temporary.directory=/dummy"""
      )

      val mt = MetaTableFactory.getDummyMetaTable(name = "table1",
        format = DataFormat.TransientEager(CachePolicy.Cache),
        infoDateColumn = infoDateColumn,
        infoDateFormat = infoDateFormat
      )

      val persistence = MetastorePersistence.fromMetaTable(mt, conf)

      assert(persistence.isInstanceOf[MetastorePersistenceTransientEager])
    }
  }

  def getParquetMtPersistence(tempDir: String,
                              recordsPerPartition: Option[Long] = None,
                              pathSuffix: String = "parquet",
                              saveModeOpt: Option[SaveMode] = None): MetastorePersistence = {

    val mt = MetaTableFactory.getDummyMetaTable(name = "table1",
      format = DataFormat.Parquet(s"$tempDir/$pathSuffix", recordsPerPartition),
      infoDateColumn = infoDateColumn,
      infoDateFormat = infoDateFormat,
      saveModeOpt = saveModeOpt
    )

    MetastorePersistence.fromMetaTable(mt, null)
  }

  def getDeltaMtPersistence(tempDir: String,
                            recordsPerPartition: Option[Long] = None,
                            pathSuffix: String = "delta",
                            saveModeOpt: Option[SaveMode] = None,
                            writeOptions: Map[String, String] = Map.empty[String, String]): MetastorePersistence = {
    val mt = MetaTableFactory.getDummyMetaTable(name = "table1",
      format = DataFormat.Delta(Query.Path(s"$tempDir/$pathSuffix"), recordsPerPartition),
      infoDateColumn = infoDateColumn,
      infoDateFormat = infoDateFormat,
      saveModeOpt = saveModeOpt,
      writeOptions = writeOptions
    )

    MetastorePersistence.fromMetaTable(mt, null)
  }

  private def getDf: DataFrame = {
    import spark.implicits._

    List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
  }
}
