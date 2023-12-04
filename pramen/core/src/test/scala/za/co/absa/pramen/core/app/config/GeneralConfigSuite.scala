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

package za.co.absa.pramen.core.app.config

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.app.config.GeneralConfig.{WRITE_OLDEST_DAYS_FROM_TODAY_KEY, WRITE_OLDEST_RUN_DATE_KEY}

import java.time.{LocalDate, ZoneId}

class GeneralConfigSuite extends AnyWordSpec {
  "GeneralConfig" should {
    "deserialize the config properly" in {
      val configStr =
        s"""pramen {
           |  environment.name = "DummyEnv"
           |  temporary.directory = "/dummy/dir"
           |  timezone = "Africa/Johannesburg"
           |  enable.multiple.jobs.per.output.table = true
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)

      val generalConfig = GeneralConfig.fromConfig(config)

      assert(generalConfig.timezoneId == ZoneId.of("Africa/Johannesburg"))
      assert(generalConfig.environmentName == "DummyEnv")
      assert(generalConfig.temporaryDirectory.contains("/dummy/dir"))
      assert(generalConfig.enableMultipleJobsPerTable)
      assert(generalConfig.writeOldestInfoDate.isEmpty)
    }

    "support the oldest date" in {
      val configStr = s"""$WRITE_OLDEST_RUN_DATE_KEY = "2023-12-14""""

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())

      val generalConfig = GeneralConfig.fromConfig(config)

      assert(generalConfig.writeOldestInfoDate.contains(LocalDate.parse("2023-12-14")))
    }

    "support the oldest day" in {
      val configStr = s"$WRITE_OLDEST_DAYS_FROM_TODAY_KEY = 30"

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())

      val generalConfig = GeneralConfig.fromConfig(config)

      assert(generalConfig.writeOldestInfoDate.contains(LocalDate.now().minusDays(30)))
    }

    "throw an exception if incompatible options are specified" in {
      val configStr =
        s"""$WRITE_OLDEST_RUN_DATE_KEY = "2023-12-14"
           |$WRITE_OLDEST_DAYS_FROM_TODAY_KEY = 30
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())

      val ex = intercept[IllegalArgumentException] {
        GeneralConfig.fromConfig(config)
      }

      assert(ex.getMessage.contains(s"Incompatible options used. Please, use only one of: $WRITE_OLDEST_RUN_DATE_KEY, $WRITE_OLDEST_DAYS_FROM_TODAY_KEY"))
    }
  }
}
