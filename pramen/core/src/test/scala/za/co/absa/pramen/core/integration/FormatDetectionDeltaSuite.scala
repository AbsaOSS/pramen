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
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.utils.ResourceUtils

import java.time.LocalDate

class FormatDetectionDeltaSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  import spark.implicits._

  private val infoDate = LocalDate.of(2021, 2, 18)
  private val exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "Format-detection" should {
    "use delta when delta data already exists" in {
      withTempDirectory("integration_file_based") { tempDir =>
        val table1BasePath = new Path(tempDir, "table1")

        exampleDf.filter(lit(false))
          .withColumn("pramen_info_date", lit(infoDate.minusDays(1)))
          .write
          .format("delta")
          .partitionBy("pramen_info_date")
          .save(table1BasePath.toString)

        val conf = getConfig(tempDir, "parquet, delta")

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val fs = table1BasePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
        assert(!fs.exists(new Path(table1BasePath, "metadata")))
        assert(!fs.exists(new Path(table1BasePath, "data")))
        assert(fs.exists(new Path(table1BasePath, "_delta_log")))

        val df = spark.read
          .format("delta")
          .load(table1BasePath.toString)

        assert(!df.isEmpty)
      }
    }
  }

  def getConfig(basePath: String, format: String): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_format_setection.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |use.format = "$format"
         |pramen.runtime.is.rerun = true
         |pramen.current.date = "$infoDate"
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
