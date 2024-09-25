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

package za.co.absa.pramen.core.tests.utils

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.exceptions.{OsSignalException, ThreadStackTrace}
import za.co.absa.pramen.core.expr.DateExprEvaluator
import za.co.absa.pramen.core.utils.StringUtils

import java.time.LocalDate
import scala.collection.JavaConverters._

class StringUtilsSuite extends AnyWordSpec {
  import StringUtils._

  "substituteVars()" should {
    val sampleString = "Var1 = ${var1}, var2 = ${var2}, var3 = ${Var3}, var4 = [ ${var4}, ${var4} ]"

    "retain the same string if no variables are provided" in {
      val s = substituteVars(sampleString, Nil)

      assert (s == sampleString)
    }

    "substitute one variable if it is provided" in {
      val s = substituteVars(sampleString, "var1" -> "A" :: Nil)

      assert (s == "Var1 = A, var2 = ${var2}, var3 = ${Var3}, var4 = [ ${var4}, ${var4} ]")
    }

    "substitute several variable if it is provided" in {
      val s = substituteVars(sampleString, "var1" -> "A" :: "var2" -> "BB" :: Nil)

      assert (s == "Var1 = A, var2 = BB, var3 = ${Var3}, var4 = [ ${var4}, ${var4} ]")
    }

    "substitute variables case-sensitively" in {
      val s = substituteVars(sampleString, "var1" -> "A" :: "var2" -> "BB" :: "var3" -> "c" :: Nil)

      assert (s == "Var1 = A, var2 = BB, var3 = ${Var3}, var4 = [ ${var4}, ${var4} ]")
    }

    "substitute multiple times" in {
      val s = substituteVars(sampleString, "var1" -> "A" :: "var2" -> "BB" :: "Var3" -> "" :: "var4" -> "d" :: Nil)

      assert (s == "Var1 = A, var2 = BB, var3 = , var4 = [ d, d ]")
    }

    "ensure no self substitutions" in {
      val ex = intercept[IllegalArgumentException] {
        substituteVars(sampleString, "var1" -> "${var1}" :: Nil)
      }

      assert (ex.getMessage.contains("Self substitutions are not allowed"))
    }

    "ensure no recursive substitutions" in {
      val ex = intercept[IllegalArgumentException] {
        substituteVars(sampleString, "var1" -> "${var2}" :: "var2" -> "${var1}" :: Nil)
      }

      assert (ex.getMessage.contains("Recursive substitutions are not allowed"))
    }
  }

  "substituteVarsNew()" should {
    val sampleString = "Var1 = ${@var1}, var2 = ${@var2}, var3 = ${@var1 + @num1}, var4 = ${yearOf(@var2)}"

    val evaluator = new DateExprEvaluator
    evaluator.setValue("var1", LocalDate.of(2020, 12, 10))
    evaluator.setValue("var2", LocalDate.of(2020, 12, 22))
    evaluator.setValue("num1", 7)

    "substitute all expressions" in {
      val s = substituteVarsNew(sampleString, evaluator)

      assert (s == "Var1 = 2020-12-10, var2 = 2020-12-22, var3 = 2020-12-17, var4 = 2020")
    }
  }

  "escapeNonAlphanumerics" should {
    "return the same string for alphanumeric strings" in {
      val str = "AbC992_11-2.bak"
      assert (escapeNonAlphanumerics(str) == str)
    }

    "do substitutions of non-alphanumeric characters" in {
      val strInput =  " AbC992 11#2*?|_"
      val strExpect = "_AbC992_11_2____"

      assert (escapeNonAlphanumerics(strInput) == strExpect)

    }
  }

  "tokenize" should {
    "return the same string is there are no whitespaces" in {
      val str = "AbC992_11-2.bak"

      val tokens = tokenize(str)
      assert (tokens.length == 1)
      assert (tokens.head == str)
    }

    "support different types of whitespaces" in {
      val strInput =  "a b\tc\nd  \te"

      val tokens = tokenize(strInput)

      assert (tokens.length == 5)
      assert (tokens.head == "a")
      assert (tokens(1) == "b")
      assert (tokens(2) == "c")
      assert (tokens(3) == "d")
      assert (tokens(4) == "e")
    }
  }

  "prettySize" should {
    "prettify bytes" in {
      assert(prettySize(1L) == "1 B")
      assert(prettySize(128L) == "128 B")
      assert(prettySize(4096L) == "4096 B")
    }

    "prettify kilobytes" in {
      assert(prettySize(13000L) == "12 KiB")
      assert(prettySize(131100L) == "128 KiB")
      assert(prettySize(4194400L) == "4096 KiB")
    }

    "prettify megabytes" in {
      assert(prettySize(12590000L) == "12 MiB")
      assert(prettySize(134220000L) == "128 MiB")
      assert(prettySize(4294968000L) == "4096 MiB")
    }

    "prettify gigabytes" in {
      assert(prettySize(12885000000L) == "12 GiB")
    }
  }

  "trimLeft" should {
    "work as expected" in {
      assert(trimLeft(null) == "")
      assert(trimLeft("") == "")
      assert(trimLeft("a") == "a")
      assert(trimLeft("  aabb") == "aabb")
      assert(trimLeft("aabb  ") == "aabb  ")
      assert(trimLeft("  aabb  ") == "aabb  ")
    }
  }

  "escapeString" should {
    "work as expected" in {
      assert(escapeString("a") == "a")
      assert(escapeString("@a") == "\"@a\"")
      assert(escapeString("@a\n\\") == "\"@a\\n\\\\\"")
      assert(escapeString("[a]") == "\"[a]\"")
      assert(escapeString("(a)") == "\"(a)\"")
      assert(escapeString("a b") == "\"a b\"")
      assert(escapeString("1+1") == "\"1+1\"")
      assert(escapeString("1-1") == "1-1")
      assert(escapeString("a#") == "\"a#\"")
      assert(escapeString("a>=b") == "\"a>=b\"")
      assert(escapeString("'a'") == "\"'a'\"")
    }
  }

  "renderThrowable" should {
    "render a throwable" in {
      val ex = new RuntimeException("test")
      val s = renderThrowable(ex)
      assert(s.contains("java.lang.RuntimeException: test"))
    }

    "render a throwable with a length limit" in {
      val ex = new RuntimeException("test")
      val s = renderThrowable(ex, maximumLength = Some(4))
      assert(s.contains("java..."))
    }

    "render a throwable with a cause" in {
      val ex = new RuntimeException("test", new RuntimeException("cause"))
      val s = renderThrowable(ex)
      assert(s.contains("java.lang.RuntimeException: test"))
      assert(s.contains("  Caused by java.lang.RuntimeException: cause"))
    }

    "render a throwable with a nested cause" in {
      val ex = new RuntimeException("test", new RuntimeException("cause", new RuntimeException("nested")))
      val s = renderThrowable(ex)
      assert(s.contains("java.lang.RuntimeException: test"))
      assert(s.contains("  Caused by java.lang.RuntimeException: cause"))
      assert(s.contains("    Caused by java.lang.RuntimeException: nested"))
    }
  }

  "renderMultiStack" should {
    "render an exception having multiple stacks" in {
      val stacks = Thread.getAllStackTraces.asScala

      val nonDaemonStackTraces = stacks.flatMap{ case (t: Thread, s: Array[StackTraceElement]) =>
        if (t.isDaemon) {
          None
        } else {
          Option(ThreadStackTrace(t.getName, s))
        }
      }.toSeq


      val ex = OsSignalException("SIGTEST", nonDaemonStackTraces)
      val actual = renderThreadDumps(ex.threadStackTraces)

      assert(actual.startsWith("Stack trace of threads at the moment of the interruption:"))
      assert(actual.contains("  Thread 0"))
      assert(actual.contains("(ScalaTest-dispatcher)"))
      assert(actual.contains("    java.lang.Thread.dumpThreads(Native Method)"))
    }
  }

  "replaceFormattedDateExpression" should {
    val infoDate = LocalDate.of(2022, 2, 18)
    val expr = new DateExprEvaluator()

    expr.setValue("date", infoDate)
    expr.setValue("dateTo", infoDate.plusDays(1))

    "work with normal variables" in {
      val template = "SELECT @dat FROM my_table_@date + 1"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "SELECT @dat FROM my_table_2022-02-18 + 1")
    }

    "work with variables at the end" in {
      val template = "SELECT @dat FROM my_table_@date"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "SELECT @dat FROM my_table_2022-02-18")
    }

    "work with just variables" in {
      val template = "@date"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "2022-02-18")
    }

    "work with 2 variables" in {
      val template = "@date @date"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "2022-02-18 2022-02-18")
    }

    "work with 2 different variables" in {
      val template = "@date @dateTo%yyyyMMdd%"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "2022-02-18 20220219")
    }

    "work with 2 different variable expressions" in {
      val template = "@{@date + 1}%yyyyMMdd% @{@dateTo + 1}%yyyyMMdd%"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "20220219 20220220")
    }

    "work with formatted variables" in {
      val template = "SELECT * FROM my_table_@date%yyyyMMdd% WHERE a = b"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "SELECT * FROM my_table_20220218 WHERE a = b")
    }

    "work with just formatted variables" in {
      val template = "@date%yyyyMMdd%"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "20220218")
    }

    "work with 2 formatted variables" in {
      val template = "@date%yyyyMMdd%@date%ddMMyyy%"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "2022021818022022")
    }

    "work with partial formatter" in {
      val template = "@date%yyyyMM%"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "202202")
    }

    "work with expressions" in {
      val template = "my_table_@{@date + 1}"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "my_table_2022-02-19")
    }

    "work with formatted expressions" in {
      val template = "my_table_@{@date + 1}%yyyyMM%"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "my_table_202202")
    }

    "work with formatted expressions 2" in {
      val template = "SELECT * FROM my_table_@{plusMonths(@date, 1)}%yyyyMMdd% WHERE a = b"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "SELECT * FROM my_table_20220318 WHERE a = b")
    }

    "work with formatted expressions 3" in {
      val template = "SELECT * FROM my_table WHERE snapshot_date = date'@{beginOfMonth(minusMonths(@date, 1))}'"

      val replaced = replaceFormattedDateExpression(template, expr)

      assert(replaced == "SELECT * FROM my_table WHERE snapshot_date = date'2022-01-01'")
    }

    "throw an exception if an expression has syntax errors" in {
      val template = "@{@date + 1}%yyyyMMdd% @{dateTo + 1}%yyyyMMdd%"

      val ex = intercept[IllegalArgumentException] {
        replaceFormattedDateExpression(template, expr)
      }

      assert(ex.getMessage.contains("Syntax error in SQL expression: @{@date + 1}%yyyyMMdd% @{dateTo + 1}%yyyyMMdd%"))
      assert(ex.getCause.getMessage.contains("Unexpected '+' at pos 7 of expression: 'dateTo + 1'"))
    }

    "throw an exception if format is incomplete" in {
      val template = "SELECT * FROM my_table WHERE snapshot_date = date'@infoDate%yyyy-mm-dd'"

      val ex = intercept[IllegalArgumentException] {
        replaceFormattedDateExpression(template, expr)
      }

      assert(ex.getMessage.contains("No matching '%' in the formatted date expression: SELECT * FROM my_table WHERE snapshot_date = date'@infoDate%yyyy-mm-dd'"))
    }

    "throw an exception if the expression is incomplete" in {
      val template = "SELECT * FROM my_table WHERE snapshot_date = date'@{beginOfMonth(minusMonths(@infoDate, 1))'"

      val ex = intercept[IllegalArgumentException] {
        replaceFormattedDateExpression(template, expr)
      }

      assert(ex.getMessage.contains("No matching '{' in the date expression: SELECT * FROM my_table WHERE snapshot_date = date'@{beginOfMonth(minusMonths(@infoDate, 1))'"))
    }
  }

}
