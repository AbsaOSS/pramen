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

import org.scalatest.WordSpec
import za.co.absa.pramen.core.expr.DateExprEvaluator
import za.co.absa.pramen.core.utils.StringUtils

import java.time.LocalDate

class StringUtilsSuite extends WordSpec {
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
    }
  }

}
