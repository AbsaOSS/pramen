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

package za.co.absa.pramen.core.config

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate

class InfoDateOverrideSuite extends AnyWordSpec {
  "fromConfig" should {
    "return all None for an empty config" in {
      val conf = ConfigFactory.empty()
      val infoDateOverride = InfoDateOverride.fromConfig(conf)

      assert(infoDateOverride.columnName.isEmpty)
      assert(infoDateOverride.dateFormat.isEmpty)
      assert(infoDateOverride.expression.isEmpty)
      assert(infoDateOverride.startDate.isEmpty)
    }

    "return the configuration when keys are set" in {
      val confStr =
        s"""information.date.column = "dummy_col"
           |information.date.format = "dummy_format"
           |information.date.expression = "dummy_expr"
           |information.date.start = "2020-01-31"
           |""".stripMargin

      val conf = ConfigFactory.parseString(confStr)

      val infoDateOverride = InfoDateOverride.fromConfig(conf)

      assert(infoDateOverride.columnName.contains("dummy_col"))
      assert(infoDateOverride.dateFormat.contains("dummy_format"))
      assert(infoDateOverride.expression.contains("dummy_expr"))
      assert(infoDateOverride.startDate.contains(LocalDate.parse("2020-01-31")))
    }

    "support days behind" in {
      val confStr = "information.date.max.days.behind = 20"

      val conf = ConfigFactory.parseString(confStr)

      val infoDateOverride = InfoDateOverride.fromConfig(conf)

      assert(infoDateOverride.startDate.contains(LocalDate.now().minusDays(20)))
    }

    "fail on incompatible options" in {
      val confStr =
        s"""information.date.start = "2020-01-31"
           |information.date.max.days.behind = 20
           |""".stripMargin

      val conf = ConfigFactory.parseString(confStr)

      val ex = intercept[IllegalArgumentException] {
        InfoDateOverride.fromConfig(conf)
      }

      assert(ex.getMessage == "Incompatible options used. Please, use only one of: information.date.start, information.date.max.days.behind.")
    }
  }
}
