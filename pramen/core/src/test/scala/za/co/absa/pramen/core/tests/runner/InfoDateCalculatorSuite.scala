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

package za.co.absa.pramen.core.tests.runner

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.expr.exceptions.SyntaxErrorException
import za.co.absa.pramen.core.runner.InfoDateCalculator

import java.time.LocalDate

class InfoDateCalculatorSuite extends AnyWordSpec {
  "calculateInformationDate" should {
    "calculate a date expression that has '$date' reference" in {
      val infoDate = InfoDateCalculator.calculateInformationDate("@date - 1", LocalDate.of(2022, 2, 1))

      assert(infoDate.toString == "2022-01-31")
    }

    "calculate a date expression that has '$runDate' reference" in {
      val infoDate = InfoDateCalculator.calculateInformationDate("beginOfMonth(@runDate) - 1", LocalDate.of(2022, 2, 22))

      assert(infoDate.toString == "2022-01-31")
    }

    "calculate a date expression that has a constant date" in {
      val infoDate = InfoDateCalculator.calculateInformationDate("'2022-01-22'", LocalDate.now())

      assert(infoDate.toString == "2022-01-22")
    }

    "throw an exception on expression parsing errors" in {
      val ex = intercept[SyntaxErrorException] {
        InfoDateCalculator.calculateInformationDate("dummy", LocalDate.now())
      }

      assert(ex.getMessage.contains("Unsupported function 'dummy'"))
    }
  }

}
