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
import za.co.absa.pramen.core.utils.ResourceUtils

import java.time.LocalDate

class ParallelExecutionLongSuite extends AnyWordSpec with SparkTestBase with TempDirFixture {
  import spark.implicits._

  private val infoDate = LocalDate.parse("2023-11-04")

  "Pipelines with parallel executions" should {
    "work for a pipeline with failures" in {
      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
      df.createOrReplaceTempView("my_table1")

      withTempDirectory("integration_parallel_executions1") { tempDir =>
        val conf = getConfig(tempDir, infoDate)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 2)

        val table3Path = new Path(tempDir, "table3")
        val sink3Path = new Path(tempDir, "sink3")

        val dfOut1 = spark.read.parquet(table3Path.toString)
        val dfOut2 = spark.read.parquet(sink3Path.toString)

        val actual1 = dfOut1.toJSON.collect()
        val actual2 = dfOut2.toJSON.collect()

        assert(actual1.length == 3)
        assert(actual1.exists(_.contains(""""a":"E"""")))
        assert(actual2.length == 3)
        assert(actual2.exists(_.contains(""""a":"E"""")))
      }

      spark.catalog.dropTempView("my_table1")
    }

    "work for a pipeline with fatal exceptions" in {
      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
      df.createOrReplaceTempView("my_table1")

      withTempDirectory("integration_parallel_executions2") { tempDir =>
        val conf = getConfig(tempDir, infoDate, fatalException = true)

        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 2)

        val table3Path = new Path(tempDir, "table3")
        val sink3Path = new Path(tempDir, "sink3")

        val dfOut1 = spark.read.parquet(table3Path.toString)
        val dfOut2 = spark.read.parquet(sink3Path.toString)

        val actual1 = dfOut1.toJSON.collect()
        val actual2 = dfOut2.toJSON.collect()

        assert(actual1.length == 3)
        assert(actual1.exists(_.contains(""""a":"E"""")))
        assert(actual2.length == 3)
        assert(actual2.exists(_.contains(""""a":"E"""")))
      }

      spark.catalog.dropTempView("my_table1")
    }
  }

  def getConfig(basePath: String, infoDate: LocalDate, fatalException: Boolean = false): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_parallel_execution.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |pramen.runtime.is.rerun = true
         |pramen.current.date = "$infoDate"
         |fatal.exception = $fatalException
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
