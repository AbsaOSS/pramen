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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{DataFormat, PartitionInfo, PartitionScheme, Query}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.metastore.peristence.MetastorePersistence
import za.co.absa.pramen.core.mocks.MetaTableFactory
import za.co.absa.pramen.core.utils.LocalFsUtils

import java.nio.file.Paths
import java.time.LocalDate
import scala.collection.mutable
import scala.util.Random

class MetastorePartitionSchemeSuite extends AnyWordSpec
  with SparkTestBase
  with TempDirFixture
  with TextComparisonFixture {

  import spark.implicits._

  "Delta Lake" when {
    val df1 = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
    val df2 = List(("D", 4)).toDF("a", "b")
    val df3 = List(("E", 5), ("F", 6)).toDF("a", "b")

    val infoDate1 = LocalDate.parse("2021-02-18")
    val infoDate2 = LocalDate.parse("2022-03-19")

    def runBasicTests(mt: MetastorePersistence, query: Query): Assertion = {
      // double write should not duplicate partitions
      mt.saveTable(infoDate1, df1, None)
      mt.saveTable(infoDate1, df1, None)

      assert(mt.loadTable(Some(infoDate1), Some(infoDate2)).count() == 3)

      mt.saveTable(infoDate2, df2, None)

      assert(mt.loadTable(None, None).count() == 4)

      query match {
        case Query.Path(path) =>
          df3.withColumn("info_date", lit(infoDate2.toString).cast(DateType))
            .write
            .format("delta")
            .mode(SaveMode.Append).save(path)
        case Query.Table(table) =>
          df3.withColumn("info_date", lit(infoDate2.toString).cast(DateType))
            .write
            .format("delta")
            .mode(SaveMode.Append).saveAsTable(table)
      }

      assert(mt.loadTable(None, None).count() == 6)
    }

    "path targets" should {
      "create, read, and append daily partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        withTempDirectory("mt_delta_part") { tempDir =>
          val mt = getDeltaMtPersistence(Query.Path(tempDir), PartitionScheme.PartitionByDay)

          runBasicTests(mt, Query.Path(tempDir))
          val df = mt.loadTable(None, None)
          assert(df.schema.fields.length == 3)

          val files = LocalFsUtils.getListOfFiles(Paths.get(tempDir), "*", includeDirs = true)
          assert(files.exists(_.toString.endsWith("/info_date=2021-02-18")))
          assert(files.exists(_.toString.endsWith("/info_date=2022-03-19")))
        }
      }

      "create, read, and append monthly partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        withTempDirectory("mt_delta_part") { tempDir =>
          val mt = getDeltaMtPersistence(Query.Path(tempDir), PartitionScheme.PartitionByMonth("info_month", "info_year"))

          runBasicTests(mt, Query.Path(tempDir))

          val df = mt.loadTable(None, None)
          assert(df.schema.fields.length == 5)

          assert(df.filter(col("info_year") === year(col("info_date"))).count() == 6)
          assert(df.filter(col("info_month") === month(col("info_date"))).count() == 6)

          val filesOuter = LocalFsUtils.getListOfFiles(Paths.get(tempDir), "*", includeDirs = true)
          assert(filesOuter.exists(_.toString.endsWith("/info_year=2021")))
          assert(filesOuter.exists(_.toString.endsWith("/info_year=2022")))
          val filesInner = LocalFsUtils.getListOfFiles(Paths.get(tempDir, "info_year=2021"), "*", includeDirs = true)
          assert(filesInner.head.toString.contains("/info_year=2021/info_month=2"))
        }
      }

      "create, read, and append year-month partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        withTempDirectory("mt_delta_part") { tempDir =>
          val mt = getDeltaMtPersistence(Query.Path(tempDir), PartitionScheme.PartitionByYearMonth("info_month"))

          runBasicTests(mt, Query.Path(tempDir))

          val df = mt.loadTable(None, None)
          assert(df.schema.fields.length == 4)

          assert(df.filter(col("info_month") === date_format(col("info_date"), "yyyy-MM")).count() == 6)

          val files = LocalFsUtils.getListOfFiles(Paths.get(tempDir), "*", includeDirs = true)
          files.foreach(println)
          assert(files.exists(_.toString.endsWith("info_month=2021-02")))
          assert(files.exists(_.toString.endsWith("info_month=2022-03")))
        }
      }

      "create, read, and append yearly partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        withTempDirectory("mt_delta_part") { tempDir =>
          val mt = getDeltaMtPersistence(Query.Path(tempDir), PartitionScheme.PartitionByYear("info_year"))

          runBasicTests(mt, Query.Path(tempDir))

          val df = mt.loadTable(None, None)
          assert(df.schema.fields.length == 4)
          assert(df.filter(col("info_year") === year(col("info_date"))).count() == 6)

          val files = LocalFsUtils.getListOfFiles(Paths.get(tempDir), "*", includeDirs = true)
          assert(files.exists(_.toString.endsWith("/info_year=2021")))
          assert(files.exists(_.toString.endsWith("/info_year=2022")))
          val filesInner = LocalFsUtils.getListOfFiles(Paths.get(tempDir, "info_year=2021"), "*", includeDirs = true)
          filesInner.exists(_.toString.endsWith(".parquet"))
        }
      }

      "create, read, and append non-partitioned tables" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        withTempDirectory("mt_delta_part") { tempDir =>
          val mt = getDeltaMtPersistence(Query.Path(tempDir), PartitionScheme.NotPartitioned)

          runBasicTests(mt, Query.Path(tempDir))

          val df = mt.loadTable(None, None)

          assert(df.schema.fields.length == 3)

          val files = LocalFsUtils.getListOfFiles(Paths.get(tempDir), "*", includeDirs = true)
          files.exists(_.toString.endsWith(".parquet"))
        }
      }
    }

    "table targets" should {
      "create, read, and append daily partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        val tableName = "mt_delta_part_table1" + Math.abs(Random.nextInt()).toString
        val mt = getDeltaMtPersistence(Query.Table(tableName), PartitionScheme.PartitionByDay)

        runBasicTests(mt, Query.Table(tableName))

        val df = mt.loadTable(None, None)
        assert(df.schema.fields.length == 3)

        val partitionColumns = getTablePartitions(tableName)

        assert(partitionColumns.length == 1)
        assert(partitionColumns.head == "info_date")

        spark.sql(s"DELETE FROM $tableName").count()
        spark.sql(s"DROP TABLE IF EXISTS $tableName").count()
      }

      "create, read, and append monthly partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        val tableName = "mt_delta_part_table2" + Math.abs(Random.nextInt()).toString
        val mt = getDeltaMtPersistence(Query.Table(tableName), PartitionScheme.PartitionByMonth("info_month", "info_year"))

        runBasicTests(mt, Query.Table(tableName))

        val df = mt.loadTable(None, None)
        assert(df.schema.fields.length == 5)

        val partitionColumns = getTablePartitions(tableName)

        assert(partitionColumns.length == 2)
        assert(partitionColumns.head == "info_year")
        assert(partitionColumns(1) == "info_month")

        spark.sql(s"DELETE FROM $tableName").count()
        spark.sql(s"DROP TABLE IF EXISTS $tableName").count()
      }

      "create, read, and append year-month partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        val tableName = "mt_delta_part_table3" + Math.abs(Random.nextInt()).toString
        val mt = getDeltaMtPersistence(Query.Table(tableName), PartitionScheme.PartitionByYearMonth("info_month"))

        runBasicTests(mt, Query.Table(tableName))

        val df = mt.loadTable(None, None)
        assert(df.schema.fields.length == 4)

        val partitionColumns = getTablePartitions(tableName)

        assert(partitionColumns.length == 1)
        assert(partitionColumns.head == "info_month")

        spark.sql(s"DELETE FROM $tableName").count()
        spark.sql(s"DROP TABLE IF EXISTS $tableName").count()
      }

      "create, read, and append yearly partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        val tableName = "mt_delta_part_table4" + Math.abs(Random.nextInt()).toString
        val mt = getDeltaMtPersistence(Query.Table(tableName), PartitionScheme.PartitionByYear("info_year"))

        runBasicTests(mt, Query.Table(tableName))

        val df = mt.loadTable(None, None)
        assert(df.schema.fields.length == 4)

        val partitionColumns = getTablePartitions(tableName)

        assert(partitionColumns.length == 1)
        assert(partitionColumns.head == "info_year")

        spark.sql(s"DELETE FROM $tableName").count()
        spark.sql(s"DROP TABLE IF EXISTS $tableName").count()
      }

      "create, read, and append non-partitioned tables" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        val tableName = "mt_delta_part_table5" + Math.abs(Random.nextInt()).toString
        val mt = getDeltaMtPersistence(Query.Table(tableName), PartitionScheme.NotPartitioned)

        runBasicTests(mt, Query.Table(tableName))

        val df = mt.loadTable(None, None)
        assert(df.schema.fields.length == 3)

        val partitionColumns = getTablePartitions(tableName)

        assert(partitionColumns.isEmpty)

        spark.sql(s"DELETE FROM $tableName").count()
        spark.sql(s"DROP TABLE IF EXISTS $tableName").count()
      }
    }
  }

  def getTablePartitions(tableName: String): Seq[String] = {
    spark.sql(s"DESCRIBE DETAIL $tableName")
      .select("partitionColumns")
      .collect()(0)(0)
      .asInstanceOf[mutable.WrappedArray[Any]]
      .toSeq.map(_.toString)
  }

  def getDeltaMtPersistence(query: Query,
                            partitionScheme: PartitionScheme = PartitionScheme.PartitionByDay): MetastorePersistence = {
    val mt = MetaTableFactory.getDummyMetaTable(name = "table1",
      format = DataFormat.Delta(query, PartitionInfo.Default),
      partitionScheme = partitionScheme,
      infoDateColumn = "info_date"
    )

    query match {
      case Query.Table(table) => spark.sql(s"DROP TABLE IF EXISTS $table").count()
      case _ => // Nothing to do
    }

    MetastorePersistence.fromMetaTable(mt, null, batchId = 0)
  }
}
