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

package za.co.absa.pramen.core.integration

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestIcebergBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.utils.ResourceUtils

import java.time.LocalDate
import scala.util.Random

class FormatDetectionIcebergSuite extends AnyWordSpec with SparkTestIcebergBase with TempDirFixture with TextComparisonFixture {
  import spark.implicits._

  private val infoDate = LocalDate.of(2021, 2, 18)
  private val exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "Format-detection" should {
    "use parquet when no tables exist and parquet is the primary format" in {
      withTempDirectory("integration_file_based") { tempDir =>
        val tableName = "mt_iceberg_detect1" + Math.abs(Random.nextInt()).toString

        val conf = getConfig(tempDir, "parquet, iceberg", tableName)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")
        val df = spark.read.parquet(table1Path.toString)

        assert(!df.isEmpty)
      }
    }

    "use delta when no tables exist and delta is the primary format" in {
      withTempDirectory("integration_file_based") { tempDir =>
        val tableName = "mt_iceberg_detect1" + Math.abs(Random.nextInt()).toString

        val conf = getConfig(tempDir, "delta, parquet", tableName)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val table1Path = new Path(tempDir, "table1")
        val df = spark.read.format("delta").load(table1Path.toString)

        assert(!df.isEmpty)
      }
    }

    "use parquet when parquet table exists" in {
      withTempDirectory("integration_file_based") { tempDir =>
        val tableName = "mt_iceberg_detect1" + Math.abs(Random.nextInt()).toString
        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=$infoDate")

        exampleDf.filter(lit(false)).write.parquet(table1Path.toString)

        val conf = getConfig(tempDir, "iceberg, parquet", tableName)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val fs = table1Path.getFileSystem(spark.sparkContext.hadoopConfiguration)
        assert(!fs.exists(new Path(table1Path, "metadata")))
        assert(!fs.exists(new Path(table1Path, "data")))
        assert(!fs.exists(new Path(table1Path, "_delta_log")))
        val df = spark.read.parquet(table1Path.toString)

        assert(!df.isEmpty)
      }
    }

    "fail when data format is not on the list" in {
      withTempDirectory("integration_file_based") { tempDir =>
        val conf = getConfig(tempDir, "iceberg, parquet")
        val table1Path = new Path(tempDir, "table1")
        exampleDf.write.format("delta").save(table1Path.toString)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 1)
      }
    }
  }

  def getConfig(basePath: String, format: String, icebergTableName: String = "abc"): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_format_setection.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |use.format = "$format"
         |iceberg.table.name = "$icebergTableName"
         |pramen.runtime.is.rerun = true
         |pramen.current.date = "$infoDate"
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
