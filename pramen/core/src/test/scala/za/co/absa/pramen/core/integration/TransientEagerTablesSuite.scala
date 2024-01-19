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

class TransientEagerTablesSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  private val infoDate = LocalDate.of(2021, 2, 18)

  "Transient metastore tables" should {
    val expected =
      """{"id":"1","name":"John"}
        |{"id":"2","name":"Jack"}
        |{"id":"3","name":"Jill"}
        |{"id":"4","name":"Mary"}
        |{"id":"5","name":"Jane"}
        |{"id":"6","name":"Kate"}
        |""".stripMargin

    "work end to end with a no_cache policy" in {
      withTempDirectory("integration_file_based") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        fsUtils.writeFile(new Path(tempDir, "landing_file1.csv"), "id,name\n1,John\n2,Jack\n3,Jill\n")
        fsUtils.writeFile(new Path(tempDir, "landing_file2.csv"), "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val conf = getConfig(tempDir)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val table3Path = new Path(new Path(tempDir, "table3"), s"pramen_info_date=$infoDate")

        assert(!fsUtils.exists(table2Path))

        val df = spark.read.parquet(table3Path.toString)
        val actual = df.orderBy("id").toJSON.collect().mkString("\n")

        compareText(actual, expected)
      }
    }

    "work end to end with a persist policy" in {
      withTempDirectory("integration_file_based") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        fsUtils.writeFile(new Path(tempDir, "landing_file1.csv"), "id,name\n1,John\n2,Jack\n3,Jill\n")
        fsUtils.writeFile(new Path(tempDir, "landing_file2.csv"), "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val conf = getConfig(tempDir, cachePolicy = "persist")

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")
        val table3Path = new Path(new Path(tempDir, "table3"), s"pramen_info_date=$infoDate")

        assert(!fsUtils.exists(table2Path))

        val df = spark.read.parquet(table3Path.toString)
        val actual = df.orderBy("id").toJSON.collect().mkString("\n")

        compareText(actual, expected)
      }
    }
  }

  def getConfig(basePath: String, cachePolicy: String = "no_cache"): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_transient_eager_transformer.conf")
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
