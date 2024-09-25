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

package za.co.absa.pramen.core.tests.expr

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.expr.DateExprEvaluator
import za.co.absa.pramen.core.expr.exceptions.SyntaxErrorException

import java.time.LocalDate
import java.time.format.DateTimeParseException

class DateExprEvaluatorSuite extends AnyWordSpec {
  private val dateFormatter = InfoDateConfig.defaultDateFormatter

  "evaluate literals" should {
    val now = LocalDate.of(2020,8, 10)
    val expr = new DateExprEvaluator
    val nowStr = now.format(dateFormatter)

    "evaluate a literal in single quotes" in {
      assert(expr.evalDate(s"'$nowStr'") == now)
    }

  }
  "evaluate dates" should {
    val now = LocalDate.of(2020,8, 10)
    val expr = new DateExprEvaluator
    expr.setValue("now", now)
    expr.setValue("two", 2)

    "check variable existence" in {
      assert(expr.contains("now"))
      assert(expr.contains("two"))
      assert(!expr.contains("dummy"))
      assert(!expr.contains("TWO"))
    }

    "get date variable directly" in {
      val actual = expr.getDate("now")

      assert(actual == now)
    }

    "get numeric variable directly" in {
      val actual = expr.getInt("two")

      assert(actual == 2)
    }

    "get Any variable directly 1" in {
      val actual = expr.getAny("now")

      assert(actual.isInstanceOf[LocalDate])
      assert(actual.asInstanceOf[LocalDate] == now)
    }

    "get Any variable directly 2" in {
      val actual = expr.getAny("two")

      assert(actual.isInstanceOf[Int])
      assert(actual.asInstanceOf[Int] == 2)
    }

    "evaluate a simple expression" in {
      assert(expr.evalDate("@now") == now)
    }

    "evaluate a simple expressions in parenthesis" in {
      assert(expr.evalDate("(@now)") == now)
    }

    "evaluate monthOf()" in {
      assert(expr.evalAny("monthOf(@now)").asInstanceOf[Int] == 8)
      assert(expr.evalAny("monthOf(@now) + 1").asInstanceOf[Int] == 9)
    }

    "evaluate yearOf()" in {
      assert(expr.evalAny("yearOf(@now)").asInstanceOf[Int] == 2020)
    }

    "evaluate yearMonthOf()" in {
      assert(expr.evalAny("yearMonthOf(@now)").asInstanceOf[String] == "2020-08")
    }

    "evaluate dayOfMonth()" in {
      assert(expr.evalAny("dayOfMonth(@now)").asInstanceOf[Int] == 10)
    }

    "evaluate dayOfWeek()" in {
      assert(expr.evalAny("dayOfWeek(@now)").asInstanceOf[Int] == 1 /* Monday */)
      assert(expr.evalAny("dayOfWeek(@now - 1)").asInstanceOf[Int] == 7 /* Sunday */)
    }

    "evaluate a plus" in {
      assert(expr.evalDate("@now + 1") == now.plusDays(1))
    }

    "evaluate a plus with parenthesis" in {
      assert(expr.evalDate("(@now + 2)") == now.plusDays(2))
    }

    "evaluate a plus with a negative number" in {
      assert(expr.evalDate("@now + -1") == now.plusDays(-1))
    }

    "evaluate a minus" in {
      assert(expr.evalDate("@now - 1") == now.minusDays(1))
    }

    "evaluate a minus with parenthesis" in {
      assert(expr.evalDate("(@now - 2)") == now.minusDays(2))
    }

    "evaluate a minus with a negative number" in {
      assert(expr.evalDate("@now - -1") == now.minusDays(-1))
    }

    "evaluate a minus with a function and negative number" in {
      assert(expr.evalDate("minusDays(@now, -1)") == now.minusDays(-1))
    }

    "evaluate a plus with a function" in {
      assert(expr.evalDate("plusDays(@now, 5)") == now.plusDays(5))
    }

    "evaluate a plus weeks" in {
      assert(expr.evalDate("plusWeeks(@now, 2)") == now.plusWeeks(2))
    }

    "evaluate a minus weeks" in {
      assert(expr.evalDate("minusWeeks(@now, 2)") == now.minusWeeks(2))
    }

    "evaluate a plus months" in {
      assert(expr.evalDate("plusMonths(@now, 2)") == now.plusMonths(2))
    }

    "evaluate a minus months" in {
      assert(expr.evalDate("minusMonths(@now, 2)") == now.minusMonths(2))
    }

    "evaluate a minus months using 2 variables" in {
      assert(expr.evalDate("minusMonths(@now, @two)") == now.minusMonths(2))
    }

    "evaluate a minus months using 2 variables and 2 numbers" in {
      assert(expr.evalDate("minusMonths(@now, (1 + (@two + 2))) + 3") == now.minusMonths(5).plusDays(3))
      assert(expr.evalDate("minusMonths(@now, 1 + @two + 2) + 3") == now.minusMonths(5).plusDays(3))
      assert(expr.evalDate("minusMonths(@now, (1 + @two) + 2) + 3") == now.minusMonths(5).plusDays(3))
    }

    "evaluate a complex expression" in {
      assert(expr.evalDate("minusMonths(plusWeeks(@now, 2), 3) - 1") == now.plusWeeks(2).minusMonths(3).minusDays(1))
    }

    "evaluate a literal" in {
      assert(expr.evalDate("'2020-10-10'") == LocalDate.of(2020, 10, 10))
    }

    "throw an exception on a non-matching single quote" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalDate("'2020-10-10")
      }
      assert(ex.getMessage.contains("No matching single quote"))
    }

    "throw an exception on a wrong date literal" in {
      val ex = intercept[DateTimeParseException] {
        expr.evalDate("'10-10-2020'")
      }
      assert(ex.getMessage.contains("could not be parsed at index"))
    }

    "throw an exception on a wrong type variable (date)" in {
      val ex = intercept[IllegalArgumentException] {
        expr.getDate("two")
      }
      assert(ex.getMessage.contains("Expected a date variable"))
    }

    "throw an exception on a wrong type variable (int)" in {
      val ex = intercept[IllegalArgumentException] {
        expr.getInt("now")
      }
      assert(ex.getMessage.contains("Expected a numeric variable"))
    }

    "throw an exception on a wrong type (date)" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalDate("minusMonths(@now, @now)")
      }
      assert(ex.getMessage.contains("Expected a number, got"))
    }

    "throw an exception on a wrong type (num)" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalDate("minusMonths(1, 1)")
      }
      assert(ex.getMessage.contains("Expected a date, got"))
    }

    "throw an exception on a wrong number of arguments" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalDate("minusMonths(@now, @now, 1)")
      }
      assert(ex.getMessage.contains("Malformed expression"))
    }

    "throw an exception on an empty expression" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalDate("()")
      }
      assert(ex.getMessage.contains("Empty expressions are not supported"))
    }

    "throw an exception if the exception is not of the expected type" in {
      val ex = intercept[IllegalArgumentException] {
        expr.evalDate("(1)")
      }
      assert(ex.getMessage.contains("Expected an expression that returns a date"))
    }

    "throw an exception if the exception is not of the expected type (2)" in {
      val ex = intercept[IllegalArgumentException] {
        expr.evalDate("1 + 1")
      }
      assert(ex.getMessage.contains("Expected an expression that returns a date"))
    }
  }

  "evaluate numbers" should {
    val expr = new DateExprEvaluator
    expr.setValue("five", 5)

    "evaluate a number" in {
      assert(expr.evalInt("1") == 1)
    }

    "evaluate a number in parenthesis" in {
      assert(expr.evalInt("(1)") == 1)
    }

    "evaluate an expression" in {
      assert(expr.evalInt("(1 - (2 + 3))") == -4)
    }

    "evaluate an expression with variables" in {
      assert(expr.evalInt("(@five - (2 + 3))") == 0)
    }

    "evaluate a complex expression" in {
      assert(expr.evalInt("((@five + 1) - (@five - 1))") == 2)
    }

    "evaluate an expression with many parenthesis" in {
      assert(expr.evalInt("(((@five) + (1)) - (@five - 1 + 1 - 2 + 2 - 1))") == 2)
    }
  }

  "evaluate period start and end functions" should {
    val now = LocalDate.of(2020,8, 12)
    val expr = new DateExprEvaluator
    expr.setValue("now", now)

    "beginOfMonth()" in {
      assert(expr.evalDate("beginOfMonth(@now)") == LocalDate.of(2020,8, 1))
    }

    "endOfMonth()" in {
      assert(expr.evalDate("endOfMonth(@now)") == LocalDate.of(2020,8, 31))
      assert(expr.evalDate("endOfMonth(minusDays(@now,5))") == LocalDate.of(2020,8, 31))
    }

    "lastDayOfMonth()" in {
      assert(expr.evalDate("lastDayOfMonth(@now, 2)") == LocalDate.of(2020,8, 2))
      assert(expr.evalDate("lastDayOfMonth(minusDays(@now, 10), 3)") == LocalDate.of(2020,7, 3))
    }
    "lastMonday()" in {
      assert(expr.evalDate("lastMonday(@now)") == LocalDate.of(2020,8, 10))
      assert(expr.evalDate("lastMonday(@now - 2)") == LocalDate.of(2020,8, 10))
    }
    "lastTuesday()" in {
      assert(expr.evalDate("lastTuesday(@now)") == LocalDate.of(2020,8, 11))
      assert(expr.evalDate("lastTuesday(@now - 2)") == LocalDate.of(2020,8, 4))
    }
    "lastWednesday()" in {
      assert(expr.evalDate("lastWednesday(@now)") == LocalDate.of(2020,8, 12))
      assert(expr.evalDate("lastWednesday(@now - 2)") == LocalDate.of(2020,8, 5))
    }
    "lastThursday()" in {
      assert(expr.evalDate("lastThursday(@now)") == LocalDate.of(2020,8, 6))
      assert(expr.evalDate("lastThursday(@now - 2)") == LocalDate.of(2020,8, 6))
    }
    "lastFriday()" in {
      assert(expr.evalDate("lastFriday(@now)") == LocalDate.of(2020,8, 7))
      assert(expr.evalDate("lastFriday(@now - 2)") == LocalDate.of(2020,8, 7))
    }
    "lastSaturday()" in {
      assert(expr.evalDate("lastSaturday(@now)") == LocalDate.of(2020,8, 8))
      assert(expr.evalDate("lastSaturday(@now - 2)") == LocalDate.of(2020,8, 8))
    }
    "lastSunday()" in {
      assert(expr.evalDate("lastSunday(@now)") == LocalDate.of(2020,8, 9))
      assert(expr.evalDate("lastSunday(@now - 2)") == LocalDate.of(2020,8, 9))
    }
  }

  "evaluate bogus expressions" should {
    val now = LocalDate.of(2020, 8, 12)
    val expr = new DateExprEvaluator
    expr.setValue("now", now)

    "when parenthesis don't match 1" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalInt("(1))")
      }
      assert(ex.getMessage.contains("Unmatched ')' at pos 3"))
    }

    "when parenthesis don't match 2" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalInt("((1)")
      }
      assert(ex.getMessage.contains("Unmatched '(' at pos 0"))
    }

    "when parenthesis don't match 3" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalInt(")(1)(")
      }
      assert(ex.getMessage.contains("Unmatched ')' at pos 0"))
    }

    "when parenthesis don't match 4" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalInt("())(1)()(")
      }
      assert(ex.getMessage.contains("Unmatched ')' at pos 2"))
    }

    "bogus comma 1" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalInt(",1 + 2")
      }
      assert(ex.getMessage.contains("Unexpected ',' at pos 0"))
    }

    "bogus comma 2" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalDate("minusDays(@now,,1)")
      }
      assert(ex.getMessage.contains("Unexpected ',' at pos 15"))
    }

    "bogus plus" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalInt("1 ++ 1")
      }
      assert(ex.getMessage.contains("Unexpected '+' at pos 3"))
    }

    "bogus minus" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalInt("1 -")
      }
      assert(ex.getMessage.contains("Expected more arguments in '1 -'"))
    }

    "unsupported function" in {
      val ex = intercept[SyntaxErrorException] {
        expr.evalInt("dummy(@now)")
      }
      assert(ex.getMessage.contains("Unsupported function 'dummy'"))
    }
  }

}
