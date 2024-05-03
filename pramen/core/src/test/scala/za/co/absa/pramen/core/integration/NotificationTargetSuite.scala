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

class NotificationTargetSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  private val infoDate = LocalDate.of(2021, 2, 18)

  "Pipeline with notification targets" should {
    val expectedSingle =
      """{"a":"D","b":4}
        |{"a":"E","b":5}
        |{"a":"F","b":6}
        |""".stripMargin

    "work end to end for non-failing pipelines" in {
      withTempDirectory("notification_targets") { tempDir =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)


        val conf = getConfig(tempDir)
        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        val table2Path = new Path(new Path(tempDir, "table2"), s"pramen_info_date=$infoDate")

        assert(fsUtils.exists(table2Path))

        val df2 = spark.read.parquet(table2Path.toString)
        val actual2 = df2.orderBy("a").toJSON.collect().mkString("\n")

        compareText(actual2, expectedSingle)

        assert(System.getProperty("pramen.test.notification.tasks.completed").toInt == 2)
        assert(System.getProperty("pramen.test.notification.table") == "table2")
      }
    }

    "still return zero exit code on notification failures" in {
      withTempDirectory("notification_targets") { tempDir =>
        val conf = getConfig(tempDir, failNotifications = true)
        val exitCode = AppRunner.runPipeline(conf)

        assert(exitCode == 0)

        assert(System.getProperty("pramen.test.notification.pipeline.failure").toBoolean)
        assert(System.getProperty("pramen.test.notification.target.failure").toBoolean)
      }
    }
  }

  def getConfig(basePath: String, failNotifications: Boolean = false): Config = {
    val configContents = ResourceUtils.getResourceString("/test/config/integration_notification_targets.conf")
    val basePathEscaped = basePath.replace("\\", "\\\\")

    val conf = ConfigFactory.parseString(
      s"""base.path = "$basePathEscaped"
         |pramen.runtime.is.rerun = true
         |pramen.current.date = "$infoDate"
         |test.fail.notification = $failNotifications
         |$configContents
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    conf
  }

}
