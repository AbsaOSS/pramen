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

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.mocks.utils.hive.QueryExecutorMock
import za.co.absa.pramen.core.utils.FsUtils
import za.co.absa.pramen.core.utils.hive.{HiveConfig, HiveHelperImpl}

class HiveHelperImplSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {

  import spark.implicits._

  private val defaultHiveConfig = HiveConfig.fromConfig(ConfigFactory.empty())

  "HiveHelperImpl" should {
    "execute expected queries for non-partitioned table" in {
      withTempDirectory("hive_test") { tempDir =>
        val path = getParquetPath(tempDir)

        val expected =
          s"""DROP TABLE IF EXISTS db.tbl
             |CREATE EXTERNAL TABLE IF NOT EXISTS
             |db.tbl ( a STRING,b INT,c INT )
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
             |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
             |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
             |
             |LOCATION '$path'
             |""".stripMargin


        val qe = new QueryExecutorMock(tableExists = false)
        val hiveHelper = new HiveHelperImpl(qe, defaultHiveConfig)
        val schema = spark.read.parquet(path).schema

        hiveHelper.createOrUpdateHiveTable(path, schema, Nil, Some("db"), "tbl")

        qe.close()

        val actual = qe.queries.mkString("\n").replaceAll("`", "")

        assert(qe.closeCalled == 1)
        compareText(actual, expected)
      }
    }

    "execute expected queries for partitioned table" in {
      withTempDirectory("hive_test") { tempDir =>
        val path = getParquetPath(tempDir)

        val expected =
          s"""DROP TABLE IF EXISTS db.tbl
             |CREATE EXTERNAL TABLE IF NOT EXISTS
             |db.tbl ( c INT )
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
             |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
             |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
             |PARTITIONED BY (a STRING,b INT)
             |LOCATION '$path'
             |MSCK REPAIR TABLE db.tbl
             |""".stripMargin


        val qe = new QueryExecutorMock(tableExists = false)
        val hiveHelper = new HiveHelperImpl(qe, defaultHiveConfig)
        val schema = spark.read.parquet(path).schema

        hiveHelper.createOrUpdateHiveTable(path, schema, "a" :: "b" :: Nil, Some("db"), "tbl")

        val actual = qe.queries.mkString("\n").replaceAll("`", "")

        compareText(actual, expected)
      }
    }

    "repair table with database" in {
      val expected = "MSCK REPAIR TABLE db.tbl"

      val qe = new QueryExecutorMock(tableExists = true)
      val hiveHelper = new HiveHelperImpl(qe, defaultHiveConfig)

      hiveHelper.repairHiveTable(Some("db"), "tbl")

      val actual = qe.queries.mkString("\n")

      compareText(actual, expected)
    }

    "repair table without database" in {
      val expected = "MSCK REPAIR TABLE tbl"

      val qe = new QueryExecutorMock(tableExists = true)
      val hiveHelper = new HiveHelperImpl(qe, defaultHiveConfig)

      hiveHelper.repairHiveTable(None, "tbl")

      val actual = qe.queries.mkString("\n")

      compareText(actual, expected)
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

}
