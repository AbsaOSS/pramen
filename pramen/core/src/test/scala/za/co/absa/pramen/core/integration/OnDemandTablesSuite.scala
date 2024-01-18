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
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.utils.{FsUtils, ResourceUtils}

import java.time.LocalDate

class OnDemandTablesSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  private val infoDate = LocalDate.of(2021, 2, 18)

  "Transient metastore tables" should {
    val expected =
      """{"a":"D","b":4}
        |{"a":"E","b":5}
        |{"a":"F","b":6}
        |""".stripMargin

    "work end to end with a no_cache policy" in {
      withTempDirectory("transient_nocache") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val conf = getConfig(tempDir)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val table2Path = new Path(tempDir, "table2")
        val table3Path = new Path(tempDir, "table3")
        val table4Path = new Path(tempDir, "table4")
        val table5Path = new Path(new Path(tempDir, "table5"), s"pramen_info_date=${infoDate.minusDays(1)}")
        val table6Path = new Path(new Path(tempDir, "table6"), s"pramen_info_date=$infoDate")
        val table7Path = new Path(new Path(tempDir, "table7"), s"pramen_info_date=$infoDate")

        assert(!fsUtils.exists(table2Path))
        assert(!fsUtils.exists(table3Path))
        assert(!fsUtils.exists(table4Path))

        val df5 = spark.read.parquet(table5Path.toString)
        val actual5 = df5.orderBy("a").toJSON.collect().mkString("\n")

        val df6 = spark.read.parquet(table6Path.toString)
        val actual6 = df6.orderBy("a").toJSON.collect().mkString("\n")

        val df7 = spark.read.parquet(table7Path.toString)
        val actual7 = df7.orderBy("a").toJSON.collect().mkString("\n")

        compareText(actual5, expected)
        compareText(actual6, expected)
        compareText(actual7, expected)
      }
    }

    "work end to end with a persist policy" in {
      withTempDirectory("transient_persist") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val conf = getConfig(tempDir, cachePolicy = "persist")

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val table2Path = new Path(tempDir, "table2")
        val table3Path = new Path(tempDir, "table3")
        val table4Path = new Path(tempDir, "table4")
        val table5Path = new Path(new Path(tempDir, "table5"), s"pramen_info_date=${infoDate.minusDays(1)}")
        val table6Path = new Path(new Path(tempDir, "table6"), s"pramen_info_date=$infoDate")
        val table7Path = new Path(new Path(tempDir, "table7"), s"pramen_info_date=$infoDate")

        assert(!fsUtils.exists(table2Path))
        assert(!fsUtils.exists(table3Path))
        assert(!fsUtils.exists(table4Path))

        val df5 = spark.read.parquet(table5Path.toString)
        val actual5 = df5.orderBy("a").toJSON.collect().mkString("\n")

        val df6 = spark.read.parquet(table6Path.toString)
        val actual6 = df6.orderBy("a").toJSON.collect().mkString("\n")

        val df7 = spark.read.parquet(table7Path.toString)
        val actual7 = df7.orderBy("a").toJSON.collect().mkString("\n")

        compareText(actual5, expected)
        compareText(actual6, expected)
        compareText(actual7, expected)
      }
    }
  }

  def getConfig(basePath: String, cachePolicy: String = "no_cache"): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_on_demand_transformer.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |cache.policy = $cachePolicy
         |pramen.runtime.is.rerun = true
         |pramen.current.date = "$infoDate"
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
