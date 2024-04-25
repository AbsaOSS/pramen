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

package za.co.absa.pramen.core.tests.utils.hive

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.utils.FsUtils
import za.co.absa.pramen.core.utils.hive.{HiveFormat, HiveHelperSparkCatalog}

class HiveHelperSparkCatalogSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {

  import spark.implicits._

  "createOrUpdateHiveTable()" should {
    "create a non-partitioned Parquet table" in {
      withTempDirectory("hive_test") { tempDir =>
        val path = getParquetPath(tempDir)

        val hiveHelper = new HiveHelperSparkCatalog(spark)
        val schema = spark.read.parquet(path).schema

        hiveHelper.createOrUpdateHiveTable(path, HiveFormat.Parquet, schema, Nil, Some("default"), "tbl1")
        assert(spark.catalog.tableExists("default.tbl1"))

        // If the table exists it should be re-created
        hiveHelper.createOrUpdateHiveTable(path, HiveFormat.Parquet, schema, Nil, Some("default"), "tbl1")
        assert(spark.catalog.tableExists("default.tbl1"))
      }
    }

    "create a partitioned Parquet table" in {
      withTempDirectory("hive_test") { tempDir =>
        val path = getParquetPath(tempDir)

        val hiveHelper = new HiveHelperSparkCatalog(spark)
        val schema = spark.read.parquet(path).withColumn("b", lit(1)).schema

        hiveHelper.createOrUpdateHiveTable(path, HiveFormat.Parquet, schema, "a" :: "b" :: Nil, Some("default"), "tbl2")
        assert(hiveHelper.doesTableExist(Some("default"),"tbl2"))

        spark.sql(s"DROP TABLE default.tbl2").collect()
        assert(!hiveHelper.doesTableExist(Some("default"),"tbl2"))

        hiveHelper.createOrUpdateHiveTable(path, HiveFormat.Parquet, schema, "a" :: "b" :: Nil, Some("default"), "tbl2")
        assert(hiveHelper.doesTableExist(Some("default"),"tbl2"))
      }
    }

    "create a partitioned Delta table" in {
      withTempDirectory("hive_test") { tempDir =>
        val path = getDeltaPath(tempDir, Seq("b"))

        val hiveHelper = new HiveHelperSparkCatalog(spark)
        val schema = spark.read.format("delta").load(path).withColumn("b", lit(1)).schema

        hiveHelper.createOrUpdateHiveTable(path, HiveFormat.Delta, schema, "b" :: Nil, Some("default"), "tbl3")
        assert(hiveHelper.doesTableExist(Some("default"), "tbl3"))

        assert(spark.table("default.tbl3").count() == 3)
      }
    }

    "drop table" in {
      withTempDirectory("hive_test") { tempDir =>
        val path = getParquetPath(tempDir)

        val hiveHelper = new HiveHelperSparkCatalog(spark)
        val schema = spark.read.parquet(path).withColumn("b", lit(1)).schema

        hiveHelper.createOrUpdateHiveTable(path, HiveFormat.Parquet, schema, "a" :: "b" :: Nil, Some("default"), "tbl4")
        assert(hiveHelper.doesTableExist(Some("default"), "tbl4"))

        hiveHelper.dropTable(Some("default"), "tbl4")

        assert(!hiveHelper.doesTableExist(Some("default"), "tbl4"))
      }
    }

    "add partition for a table in a database" in {
      withTempDirectory("hive_test") { tempDir =>
        val path = getParquetPath(tempDir, Seq("b"))

        val hiveHelper = new HiveHelperSparkCatalog(spark)
        val schema = spark.read.parquet(path).withColumn("b", lit(1)).schema

        hiveHelper.createOrUpdateHiveTable(path, HiveFormat.Parquet, schema, "a" :: "b" :: Nil, Some("default"), "tbl5")
        assert(hiveHelper.doesTableExist(Some("default"),"tbl5"))

        val df = List(("D", 40)).toDF("a", "c")
        df.write.parquet(s"$path/b=4")

        assert(spark.table("default.tbl5").count() == 3)

        hiveHelper.addPartition(Some("default"),"tbl5", Seq("b"), Seq("4"), s"$path/b=4")

        assert(spark.table("default.tbl5").count() == 4)

        hiveHelper.dropTable(Some("default"),"tbl5")
      }
    }
  }

  private def getParquetPath(tempBaseDir: String, partitionBy: Seq[String] = Nil): String = {
    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, "file:///")

    val tempDir = fsUtils.getTempPath(new Path(tempBaseDir)).toString

    val df = List(("A", 1, 10), ("B", 2, 20), ("C", 3, 30)).toDF("a", "b", "c")

    if (partitionBy.isEmpty) {
      df.repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(tempDir)
    } else {
      df.repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy(partitionBy: _*)
        .parquet(tempDir)
    }

    tempDir
  }

  private def getDeltaPath(tempBaseDir: String, partitionBy: Seq[String]): String = {
    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, "file:///")

    val tempDir = fsUtils.getTempPath(new Path(tempBaseDir)).toString

    val df = List(("A", 1, 10), ("B", 2, 20), ("C", 3, 30)).toDF("a", "b", "c")

    df.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionBy: _*)
      .format("delta")
      .save(tempDir)

    tempDir
  }

}
