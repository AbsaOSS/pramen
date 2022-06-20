/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.utils

import za.co.absa.pramen.framework.expr.DateExprEvaluator

import java.util.StringTokenizer
import scala.util.control.NonFatal

object StringUtils {

  /**
    * Substitutes variables in form of \${var} with values in a given string.
    *
    * @param inputString   A String.
    * @param substitutions A list of substitution key value pairs.
    * @return A new string with all substitutions applied.
    */
  def substituteVars(inputString: String, substitutions: Seq[(String, String)]): String = {
    val values = substitutions.map(_._2)
    substitutions.foldLeft(inputString) { case (acc, (key, value)) =>
      val variable = s"$${$key}"
      if (variable == value) {
        throw new IllegalArgumentException(s"Self substitutions are not allowed in $key = $value.")
      }
      if (values.contains(variable)) {
        throw new IllegalArgumentException(s"Recursive substitutions are not allowed in $key = $value.")
      }

      acc.replaceAllLiterally(variable, value)
    }
  }

  /**
    * Substitutes variables in form of `\${@var + 1}` with values in a given string.
    *
    * @param inputString A String.
    * @param evaluator   An expression evaluator.
    * @return A new string with all substitutions applied.
    */
  def substituteVarsNew(inputString: String, evaluator: DateExprEvaluator): String = {
    def substituteOneVar(s: String): String = {
      val ind0 = s.indexOf("${")

      if (ind0 >= 0) {
        val ind1 = s.indexOf("}", ind0)
        if (ind1 < 0) {
          throw new IllegalArgumentException(s"Unmatched open '{' at pos $ind0 of expression: $inputString")
        }

        val expr = s.substring(ind0 + 2, ind1)
        try {
          val v = evaluator.evalAny(expr).toString
          s"${s.take(ind0)}$v${s.substring(ind1 + 1)}"
        } catch {
          case NonFatal(ex) => throw new IllegalArgumentException(s"Cannot evaluate expression '$expr' in '$inputString'", ex)
        }
      } else {
        inputString
      }
    }

    var s = inputString
    while (s.contains("${")) {
      s = substituteOneVar(s)
    }
    s
  }

  /**
    * Substitutes non-alphanumeric characters with underscores(_).
    *
    * @param s A String.
    * @return A new string with all substitutions applied.
    */
  def escapeNonAlphanumerics(s: String): String = {
    val allowedCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-.".toCharArray.toSet
    s.map(c => if (allowedCharacters.contains(c)) c else "_").mkString
  }

  /**
    * Split a string by whitespace characters. It follows command line parsing rules.
    *
    * @param s A String.
    * @return An array of tokens.
    */
  def tokenize(s: String): Array[String] = {
    val st = new StringTokenizer(s)
    val array = new Array[String](st.countTokens)
    var i = 0
    while (st.hasMoreTokens) {
      array(i) = st.nextToken
      i += 1
    }
    array
  }

  /** Prettify a value of size in bytes */
  def prettySize(sizeBytes: Long): String = {
    if (sizeBytes < 10L * 1024L) {
      s"$sizeBytes B"
    } else if (sizeBytes < 10L * 1024L * 1024L) {
      s"${sizeBytes / 1024} KiB"
    } else if (sizeBytes < 10L * 1024L * 1024L * 1024L) {
      s"${sizeBytes / (1024L*1024L)} MiB"
    } else {
      s"${sizeBytes / (1024L*1024L*1024L)} GiB"
    }
  }

  /**
    * Trims a string from the left side
    * (The implementation is based on java.lang.String.trim())
    *
    * @param s A string
    * @return The trimmed string
    */
  final def trimLeft(s: String): String = {
    if (s == null) return ""

    val len = s.length
    var st = 0
    val v = s.toCharArray

    while ( {
      (st < len) && (v(st) <= ' ')
    }) st += 1

    if ((st > 0) || (len < s.length))
      s.substring(st, len)
    else s
  }

  /** Wraps in quotes and escape special characters if necessary. */
  def escapeString(s: String): String = {
    if (s.forall(a => !" =<>#*+~`@!&*()[]{}\\\n\"".contains(a)))
      s"$s"
    else {
      // Escape quotes, back slashed and line ending characters
      val q = "\""
      val str = s
        .replaceAll("\\\\", "\\\\\\\\")
        .replaceAll("\"", "\\\\\"")
        .replaceAll("\n", "\\\\n")
      s"$q$str$q"
    }
  }

}
