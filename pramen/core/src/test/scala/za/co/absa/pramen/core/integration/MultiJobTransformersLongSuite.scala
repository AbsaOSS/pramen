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
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.app.config.GeneralConfig.ENABLE_MULTIPLE_JOBS_PER_OUTPUT_TABLE
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.utils.ResourceUtils

import java.time.LocalDate

class MultiJobTransformersLongSuite extends AnyWordSpec with SparkTestBase with TempDirFixture {
  import spark.implicits._

  private val infoDateSaturday = LocalDate.parse("2023-11-04")
  private val infoDateSunday = LocalDate.parse("2023-11-05")
  private val infoDateMonday = LocalDate.parse("2023-11-06")

  "Multi job per table transformers File-based sink from a raw metastore table" should {
    "work for a day without overlapping info dates" in {
      withTempDirectory("integration_multi_table_transformers") { tempDir =>
        val conf = getConfig(tempDir, infoDateSaturday, enableMultiJobPerOutputTable = true)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val metastorePath = new Path(tempDir, "table1")
        val sinkPath = new Path(tempDir, "sink")

        val dfOut1 = spark.read.parquet(metastorePath.toString)
        val dfOut2 = spark.read.parquet(sinkPath.toString)

        val actual1 = dfOut1.toJSON.collect()
        val actual2 = dfOut2.toJSON.collect()

        assert(actual1.length == 3)
        assert(actual1.exists(_.contains(""""a":"E"""")))
        assert(actual2.length == 3)
        assert(actual2.exists(_.contains(""""a":"E"""")))
      }
    }

    "work for a day with a different job (source)" in {
      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
      df.createOrReplaceTempView("my_table1")

      withTempDirectory("integration_multi_table_transformers") { tempDir =>

        val conf = getConfig(tempDir, infoDateMonday, enableMultiJobPerOutputTable = true)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val metastorePath = new Path(tempDir, "table1")
        val sinkPath = new Path(tempDir, "sink")

        val dfOut1 = spark.read.parquet(metastorePath.toString)
        val dfOut2 = spark.read.parquet(sinkPath.toString)

        val actual1 = dfOut1.toJSON.collect()
        val actual2 = dfOut2.toJSON.collect()

        assert(actual1.length == 3)
        assert(actual1.exists(_.contains(""""a":"A"""")))
        assert(actual2.length == 3)
        assert(actual2.exists(_.contains(""""a":"A"""")))
      }

      spark.catalog.dropTempView("my_table1")
    }

    "fail if multi jobs per output table is not enabled" in {
      withTempDirectory("integration_multi_table_transformers") { tempDir =>
        val conf = getConfig(tempDir, infoDateSaturday)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 1)
      }
    }

    "fail if multiple jobs that output to the same table and info date" in {
      withTempDirectory("integration_multi_table_transformers") { tempDir =>
        val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

        df.createOrReplaceTempView("my_table1")

        val conf = getConfig(tempDir, infoDateSunday, enableMultiJobPerOutputTable = true)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 1)
      }
    }
  }

  def getConfig(basePath: String, infoDate: LocalDate, enableMultiJobPerOutputTable: Boolean = false): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_multi_table_transformers.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |pramen.runtime.is.rerun = true
         |pramen.current.date = "$infoDate"
         |$ENABLE_MULTIPLE_JOBS_PER_OUTPUT_TABLE = $enableMultiJobPerOutputTable
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
