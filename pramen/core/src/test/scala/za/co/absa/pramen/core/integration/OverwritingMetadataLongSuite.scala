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

class OverwritingMetadataLongSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  private val infoDate = LocalDate.of(2021, 2, 18)

  "File-based sourcing" should {
    val expected1 =
      """{"id":"1","name":"John","pramen_info_date":"2021-02-18"}
        |{"id":"2","name":"Jack","pramen_info_date":"2021-02-18"}
        |{"id":"3","name":"Jill","pramen_info_date":"2021-02-18"}
        |{"id":"4","name":"Mary","pramen_info_date":"2021-02-18"}
        |{"id":"5","name":"Jane","pramen_info_date":"2021-02-18"}
        |{"id":"6","name":"Kate","pramen_info_date":"2021-02-18"}
        |""".stripMargin

    val expected2 =
      """{"id":"1","name":"John"}
        |{"id":"2","name":"Jack"}
        |{"id":"3","name":"Jill"}
        |{"id":"4","name":"Mary"}
        |{"id":"5","name":"Jane"}
        |{"id":"6","name":"Kate"}
        |""".stripMargin

    "work end to end" in {
      withTempDirectory("integration_overwriting") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        fsUtils.writeFile(new Path(tempDir, "landing_file1.csv"), "id,name\n1,John\n2,Jack\n3,Jill\n")
        fsUtils.writeFile(new Path(tempDir, "landing_file2.csv"), "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val conf = getConfig(tempDir)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val table2Path = new Path(tempDir, "table2")
        val table2PartitionPath = new Path(table2Path, s"pramen_info_date=$infoDate")

        val fs = table2Path.getFileSystem(spark.sparkContext.hadoopConfiguration)

        assert(!fs.exists(table2PartitionPath))

        val df1 = spark.read.parquet(table2Path.toString)
        val actual1 = df1.orderBy("id").toJSON.collect().mkString("\n")

        compareText(actual1, expected1)

        val table3PartitionPath = new Path(new Path(tempDir, "table3"), s"pramen_info_date=${infoDate.minusDays(1)}")
        val df2 = spark.read.parquet(table3PartitionPath.toString)
        val actual2 = df2.orderBy("id").toJSON.collect().mkString("\n")

        compareText(actual2, expected2)

        val sinkPath = new Path(tempDir, "sink")
        val df3 = spark.read.parquet(sinkPath.toString)
        val actual3 = df3.orderBy("id").toJSON.collect().mkString("\n")

        compareText(actual3, expected1)
      }
    }
  }

  def getConfig(basePath: String): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_overwriting_metatable.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |pramen.runtime.is.rerun = true
         |pramen.current.date = "$infoDate"
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
