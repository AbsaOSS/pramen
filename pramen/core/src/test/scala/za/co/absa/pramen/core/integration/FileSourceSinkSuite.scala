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
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.runner.AppRunner
import za.co.absa.pramen.core.utils.{FsUtils, ResourceUtils}

import java.time.LocalDate

class FileSourceSinkSuite extends AnyWordSpec with SparkTestBase with TempDirFixture {
  private val infoDate = LocalDate.of(2021, 2, 18)

  "File-based sink from a raw metastore table" should {
    "work end to end with path-based transformer" in {
      withTempDirectory("integration_file_based") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

        fsUtils.writeFile(new Path(tempDir, "landing_file1.csv"), "id,name\n1,John\n2,Jack\n3,Jill\n")
        fsUtils.writeFile(new Path(tempDir, "landing_file2.csv"), "id,name\n4,Mary\n5,Jane\n6,Kate\n")

        val conf = getConfig(tempDir)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val sinkPath = new Path(tempDir, "sink")
        val df = spark.read.parquet(sinkPath.toString)

        val actual = df.toJSON.collect()

        assert(actual.length == 2)
        assert(actual.exists(_.contains("landing_file1.csv")))
        assert(actual.exists(_.contains("landing_file2.csv")))
      }
    }
  }

  def getConfig(basePath: String): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_file_source_sink.conf")
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
