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
import za.co.absa.pramen.core.app.config.InfoDateConfig.{INFORMATION_DATE_START_DAYS_KEY, INFORMATION_DATE_START_KEY}

import java.time.LocalDate

class InfoDateConfigSuite extends AnyWordSpec {
  "InfoDateConfig" should {
    "deserialize the config properly when set" in {
      val configStr =
        s"""pramen {
           |  information.date.column = "dummy_col"
           |  information.date.format = "dummy_format"
           |  information.date.start = "2020-01-31"
           |
           |  track.days = 4
           |  expected.delay.days =  0
           |
           |  default.daily.output.info.date.expr = "expr1"
           |  default.weekly.output.info.date.expr = "expr2"
           |  default.monthly.output.info.date.expr = "expr3"
           |
           |  initial.sourcing.date.daily.expr = "expr4"
           |  initial.sourcing.date.weekly.expr= "expr5"
           |  initial.sourcing.date.monthly.expr = "expr6"
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)

      val runtimeConfig = InfoDateConfig.fromConfig(config)

      assert(runtimeConfig.columnName == "dummy_col")
      assert(runtimeConfig.dateFormat == "dummy_format")
      assert(runtimeConfig.startDate.toString == "2020-01-31")

      assert(runtimeConfig.defaultTrackDays == 4)
      assert(runtimeConfig.defaultDelayDays == 0)

      assert(runtimeConfig.expressionDaily == "expr1")
      assert(runtimeConfig.expressionWeekly == "expr2")
      assert(runtimeConfig.expressionMonthly == "expr3")
      assert(runtimeConfig.initialSourcingDateExprDaily == "expr4")
      assert(runtimeConfig.initialSourcingDateExprWeekly == "expr5")
      assert(runtimeConfig.initialSourcingDateExprMonthly == "expr6")
    }

    "have default values" in {
      val config = ConfigFactory.load()

      val runtimeConfig = InfoDateConfig.fromConfig(config)

      assert(runtimeConfig.columnName == "pramen_info_date")
      assert(runtimeConfig.dateFormat == "yyyy-MM-dd")

      assert(runtimeConfig.expressionDaily == "@runDate")
      assert(runtimeConfig.expressionWeekly == "lastMonday(@runDate)")
      assert(runtimeConfig.expressionMonthly == "beginOfMonth(@runDate)")
    }

    "parse date in custom format" in {
      val configStr =
        s"""pramen {
           |  information.date.column = "dummy_col"
           |  information.date.format = "yyyyMMdd"
           |  information.date.start = "20200131"
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
            .withFallback(ConfigFactory.load())

      val runtimeConfig = InfoDateConfig.fromConfig(config)

      assert(runtimeConfig.columnName == "dummy_col")
      assert(runtimeConfig.startDate.toString == "2020-01-31")
    }

    "parse date in internal format" in {
      val configStr =
        s"""pramen {
           |  information.date.column = "dummy_col"
           |  information.date.format = "yyyyMMdd"
           |  information.date.start = "2020-01-31"
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())

      val runtimeConfig = InfoDateConfig.fromConfig(config)

      assert(runtimeConfig.columnName == "dummy_col")
      assert(runtimeConfig.startDate.toString == "2020-01-31")
    }

    "throw an exception on parsing errors" in {
      val configStr =
        s"""pramen {
           |  information.date.column = "dummy_col"
           |  information.date.format = "yyyyMMdd"
           |  information.date.start = "123"
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())

      val ex = intercept[IllegalArgumentException] {
        InfoDateConfig.fromConfig(config)
      }

      assert(ex.getMessage.contains("Cannot parse '123' in one of 'yyyy-MM-dd', 'yyyyMMdd'"))
    }

    "support the default start date" in {
      val config = ConfigFactory.load()

      val infoDateConfig = InfoDateConfig.fromConfig(config)

      assert(infoDateConfig.startDate == LocalDate.parse("2010-01-01"))
    }

    "support the start date" in {
      val configStr = s"""$INFORMATION_DATE_START_KEY = "2023-12-14""""

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())

      val infoDateConfig = InfoDateConfig.fromConfig(config)

      assert(infoDateConfig.startDate == LocalDate.parse("2023-12-14"))
    }

    "support the start day" in {
      val configStr = s"$INFORMATION_DATE_START_DAYS_KEY = 30"

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())

      val infoDateConfig = InfoDateConfig.fromConfig(config)

      assert(infoDateConfig.startDate == LocalDate.now().minusDays(30))
    }

    "throw an exception if incompatible options are specified" in {
      val configStr =
        s"""$INFORMATION_DATE_START_KEY = "2023-12-14"
           |$INFORMATION_DATE_START_DAYS_KEY = 30
           |""".stripMargin

      val config = ConfigFactory.parseString(configStr)
        .withFallback(ConfigFactory.load())

      val ex = intercept[IllegalArgumentException] {
        InfoDateConfig.fromConfig(config)
      }

      assert(ex.getMessage.contains(s"Incompatible options used. Please, use only one of: $INFORMATION_DATE_START_KEY, $INFORMATION_DATE_START_DAYS_KEY"))
    }
  }

}
