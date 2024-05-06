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

package za.co.absa.pramen.core.utils

import za.co.absa.pramen.core.exceptions.ThreadStackTrace
import za.co.absa.pramen.core.expr.DateExprEvaluator

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Base64, StringTokenizer}
import scala.compat.Platform.EOL
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

      acc.replace(variable, value)
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
    if (s.forall(a => !" =<>#*+~`'@!&*()[]{}\\\n\"".contains(a)))
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

  def encodeToBase64(string: String): String = {
    Base64.getEncoder.encodeToString(string.getBytes)
  }

  /** Renders an exception as a string */
  def renderThrowable(ex: Throwable, level: Int = 1): String = {
    val prefix = " " * (level * 2)
    val base = s"""${ex.toString}\n${ex.getStackTrace.map(s => s"$prefix$s").mkString("", EOL, EOL)}"""
    val cause = Option(ex.getCause) match {
      case Some(c) if level < 6 => s"\n${prefix}Caused by " + renderThrowable(c, level + 1)
      case _                    => ""
    }
    base + cause
  }

  def renderThreadDumps(threadStackTraces: Seq[ThreadStackTrace]): String = {
    val threadTitlePadding = "  "
    val stackTracePadding = "    "
    val base = s"""Stack trace of threads at the moment of the interruption:\n"""

    val details = threadStackTraces.zipWithIndex.map {
      case (threadStackTrace, index) =>
        val threadTitle = s"${threadTitlePadding}Thread $index (${threadStackTrace.threadName}): \n"
        val stackTrace = threadStackTrace.stackTrace
        val stackTraceStr = s"""${stackTrace.map(s => s"$stackTracePadding$s").mkString("", EOL, EOL)}""".stripMargin
        threadTitle + stackTraceStr
    }.mkString("\n")

    base + details
  }

  def replaceFormattedDate(template: String, dateVar: String, date: LocalDate): String = {
    val output = new StringBuilder()
    val outputPartial = new StringBuilder()
    var state = 0
    var i = 0
    var j = 0

    val STATE_TEMPLATE_AS_IS = 0
    val CATCH_VARIABLE = 1
    val END_OF_VARIABLE = 2
    val END_OF_FORMAT = 3

    while (i < template.length) {
      val c = template(i)
      state match {
        case STATE_TEMPLATE_AS_IS =>
          if (c == dateVar(0)) {
            state = CATCH_VARIABLE
            j = 1
            outputPartial.clear()
            outputPartial.append(s"$c")
          } else {
            output.append(s"$c")
          }
        case CATCH_VARIABLE =>
          outputPartial.append(s"$c")
          if (c == dateVar(j)) {
            j += 1
            if (j == dateVar.length) {
              state = END_OF_VARIABLE
              if (i == template.length - 1) {
                output.append(s"$date")
              }
            }
          } else {
            output.append(s"${outputPartial.toString()}")
            outputPartial.clear()
            state = STATE_TEMPLATE_AS_IS
          }
        case END_OF_VARIABLE =>
          if (c == '%') {
            state = END_OF_FORMAT
            outputPartial.clear()
          } else {
            output.append(s"$date$c")
            state = STATE_TEMPLATE_AS_IS
          }
        case END_OF_FORMAT =>
          if (c == '%') {
            state = STATE_TEMPLATE_AS_IS
            val formatter = DateTimeFormatter.ofPattern(outputPartial.toString())
            output.append(s"${formatter.format(date)}")
          } else {
            outputPartial.append(s"$c")
          }
      }
      i += 1
    }
    output.toString()
  }


}
