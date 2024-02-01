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

class TransientTablesSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  private val infoDate = LocalDate.of(2021, 2, 18)

  "Lazy transient tables" should {
    val expectedSingle =
      """{"a":"D","b":4}
        |{"a":"E","b":5}
        |{"a":"F","b":6}
        |""".stripMargin

    val expectedDouble =
      """{"a":"A","b":1}
        |{"a":"B","b":2}
        |{"a":"C","b":3}
        |{"a":"D","b":4}
        |{"a":"E","b":5}
        |{"a":"F","b":6}
        |""".stripMargin

    import spark.implicits._

    val exampleDf = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

    "work end to end with a no_cache policy" in {
      withTempDirectory("transient_nocache") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=${infoDate.minusDays(4)}")
        exampleDf.write.parquet(table1Path.toString)

        val conf = getConfig(tempDir)
        val exitCode = AppRunner.runPipeline(conf)
        assert(exitCode == 0)

        val table2Path = new Path(tempDir, "table2")
        val table3Path = new Path(new Path(tempDir, "table3"), s"pramen_info_date=${infoDate.minusDays(1)}")
        val table4Path = new Path(new Path(tempDir, "table4"), s"pramen_info_date=$infoDate")
        val table5Path = new Path(new Path(tempDir, "table5"), s"pramen_info_date=$infoDate")

        assert(!fsUtils.exists(table2Path))

        val df3 = spark.read.parquet(table3Path.toString)
        val actual3 = df3.orderBy("a").toJSON.collect().mkString("\n")

        val df4 = spark.read.parquet(table4Path.toString)
        val actual4 = df4.orderBy("a").toJSON.collect().mkString("\n")

        val df5 = spark.read.parquet(table5Path.toString)
        val actual5 = df5.orderBy("a").toJSON.collect().mkString("\n")

        compareText(actual3, expectedSingle)
        compareText(actual4, expectedDouble)
        compareText(actual5, expectedSingle)
      }
    }

    "work end to end with a persist policy" in {
      withTempDirectory("transient_persist") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        val table1Path = new Path(new Path(tempDir, "table1"), s"pramen_info_date=${infoDate.minusDays(4)}")
        exampleDf.write.parquet(table1Path.toString)

        val conf = getConfig(tempDir, cachePolicy = "persist")
        val exitCode = AppRunner.runPipeline(conf)
        assert(exitCode == 0)

        val table2Path = new Path(tempDir, "table2")
        val table3Path = new Path(new Path(tempDir, "table3"), s"pramen_info_date=${infoDate.minusDays(1)}")
        val table4Path = new Path(new Path(tempDir, "table4"), s"pramen_info_date=$infoDate")
        val table5Path = new Path(new Path(tempDir, "table5"), s"pramen_info_date=$infoDate")

        assert(!fsUtils.exists(table2Path))

        val df3 = spark.read.parquet(table3Path.toString)
        val actual3 = df3.orderBy("a").toJSON.collect().mkString("\n")

        val df4 = spark.read.parquet(table4Path.toString)
        val actual4 = df4.orderBy("a").toJSON.collect().mkString("\n")

        val df5 = spark.read.parquet(table5Path.toString)
        val actual5 = df5.orderBy("a").toJSON.collect().mkString("\n")

        compareText(actual3, expectedSingle)
        compareText(actual4, expectedDouble)
        compareText(actual5, expectedSingle)
      }
    }
  }

  def getConfig(basePath: String, cachePolicy: String = "no_cache"): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_transient_transformer.conf")
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
