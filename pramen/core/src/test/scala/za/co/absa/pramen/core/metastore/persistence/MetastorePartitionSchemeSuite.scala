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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, lit, month, year}
import org.apache.spark.sql.types.DateType
import org.scalatest.{Assertion, BeforeAndAfter, BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{DataFormat, PartitionInfo, PartitionScheme, Query}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.metastore.peristence.MetastorePersistence
import za.co.absa.pramen.core.mocks.MetaTableFactory
import za.co.absa.pramen.core.utils.LocalFsUtils

import java.nio.file.Paths
import java.time.LocalDate

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

    def runBasicTests(mt: MetastorePersistence, tempDir: String): Assertion = {
      // double write should not duplicate partitions
      mt.saveTable(infoDate1, df1, None)
      mt.saveTable(infoDate1, df1, None)

      assert(mt.loadTable(Some(infoDate1), Some(infoDate2)).count() == 3)

      mt.saveTable(infoDate2, df2, None)

      assert(mt.loadTable(None, None).count() == 4)

      df3.withColumn("info_date", lit(infoDate2.toString).cast(DateType))
        .write
        .format("delta")
        .mode(SaveMode.Append).save(new Path(tempDir).toString)

      assert(mt.loadTable(None, None).count() == 6)
    }

    "path targets" should {
      "create, read, and append daily partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        withTempDirectory("mt_delta_part") { tempDir =>
          val mt = getDeltaMtPersistence(tempDir, PartitionScheme.PartitionByDay)

          runBasicTests(mt, tempDir)
          val df = mt.loadTable(None, None)
          assert(df.schema.fields.length == 3)

          val files = LocalFsUtils.getListOfFiles(Paths.get(tempDir), "*", includeDirs = true)
          assert(files.exists(_.toString.contains("/info_date=2021-02-18")))
          assert(files.exists(_.toString.contains("/info_date=2022-03-19")))
        }
      }

      "create, read, and append monthly partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        withTempDirectory("mt_delta_part") { tempDir =>
          val mt = getDeltaMtPersistence(tempDir, PartitionScheme.PartitionByMonth("info_month", "info_year"))

          runBasicTests(mt, tempDir)

          val df = mt.loadTable(None, None)
          assert(df.schema.fields.length == 5)

          assert(df.filter(col("info_year") === year(col("info_date"))).count() == 6)
          assert(df.filter(col("info_month") === month(col("info_date"))).count() == 6)

          val filesOuter = LocalFsUtils.getListOfFiles(Paths.get(tempDir), "*", includeDirs = true)
          assert(filesOuter.exists(_.toString.contains("/info_year=2021")))
          assert(filesOuter.exists(_.toString.contains("/info_year=2022")))
          val filesInner = LocalFsUtils.getListOfFiles(Paths.get(tempDir, "info_year=2021"), "*", includeDirs = true)
          assert(filesInner.head.toString.contains("/info_year=2021/info_month=2"))
        }
      }

      "create, read, and append yearly partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        withTempDirectory("mt_delta_part") { tempDir =>
          val mt = getDeltaMtPersistence(tempDir, PartitionScheme.PartitionByYear("info_year"))

          runBasicTests(mt, tempDir)

          val df = mt.loadTable(None, None)
          assert(df.schema.fields.length == 4)
          assert(df.filter(col("info_year") === year(col("info_date"))).count() == 6)

          val files = LocalFsUtils.getListOfFiles(Paths.get(tempDir), "*", includeDirs = true)
          assert(files.exists(_.toString.contains("/info_year=2021")))
          assert(files.exists(_.toString.contains("/info_year=2022")))
          val filesInner = LocalFsUtils.getListOfFiles(Paths.get(tempDir, "info_year=2021"), "*", includeDirs = true)
          filesInner.exists(_.toString.endsWith(".parquet"))
        }
      }

      "create, read, and append non-partitioned tables" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

        withTempDirectory("mt_delta_part") { tempDir =>
          val mt = getDeltaMtPersistence(tempDir, PartitionScheme.NotPartitioned)

          runBasicTests(mt, tempDir)

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

      }

      "create, read, and append monthly partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

      }

      "create, read, and append yearly partitions" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

      }

      "create, read, and append non-partitioned tables" in {
        assume(spark.version.split('.').head.toInt >= 3, s"Ignored for too old Delta Lake for Spark ${spark.version}")

      }
    }
  }

  def getDeltaMtPersistence(tempDir: String,
                            partitionScheme: PartitionScheme = PartitionScheme.PartitionByDay): MetastorePersistence = {
    val mt = MetaTableFactory.getDummyMetaTable(name = "table1",
      format = DataFormat.Delta(Query.Path(tempDir), PartitionInfo.Default),
      partitionScheme = partitionScheme,
      infoDateColumn = "info_date"
    )

    MetastorePersistence.fromMetaTable(mt, null, batchId = 0)
  }
}
