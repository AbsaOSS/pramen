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

package za.co.absa.pramen.core.expr

import java.time.LocalDate

import za.co.absa.pramen.core.expr.lexer.Lexer
import za.co.absa.pramen.core.expr.parser.{DateExpressionEvaluator, Parser}

import scala.collection.mutable

/**
  * This class evaluates expressions involving dates.
  * Expressions can use variables that have '@' prefix.
  *
  * Example:
  *   {{{
  *   val expr = new DateExprEvaluator
  *   expr.setValue("now", now)
  *   expr.setValue("two", 2)
  *
  *   assert(expr.evalDate("plusDays(@now, @two - 1)") == now.plusDays(1))
  *   }}}
  */
class DateExprEvaluator {
  private val vars = mutable.HashMap[String, Any]()

  def setValue(varName: String, value: LocalDate): Unit = {
    vars += varName -> value
  }

  def setValue(varName: String, value: Int): Unit = {
    vars += varName -> value
  }

  def evalDate(expression: String): LocalDate = {
    val tokens = new Lexer(expression).lex()

    val exprBuilder = new DateExpressionEvaluator(vars.toMap, expression)
    new Parser(expression, tokens, exprBuilder).parse()

    exprBuilder.getResult match {
      case d: LocalDate => d
      case n => throw new IllegalArgumentException(s"Expected an expression that returns a date, but a number ($n) is returned in $expression.")
    }
 }

  def evalInt(expression: String): Int = {
    val tokens = new Lexer(expression).lex()

    val exprBuilder = new DateExpressionEvaluator(vars.toMap, expression)
    new Parser(expression, tokens, exprBuilder).parse()

    exprBuilder.getResult match {
      case n: Int => n
      case d => throw new IllegalArgumentException(s"Expected an expression that returns a number, but a date ($d) is returned in $expression.")
    }
  }

  def evalAny(expression: String): Any = {
    val tokens = new Lexer(expression).lex()

    val exprBuilder = new DateExpressionEvaluator(vars.toMap, expression)
    new Parser(expression, tokens, exprBuilder).parse()

    exprBuilder.getResult
  }

  def contains(variable: String): Boolean = {
    vars.contains(variable)
  }

  def getInt(variable: String): Int = {
    vars(variable) match {
      case n: Int => n
      case d => throw new IllegalArgumentException(s"Expected a numeric variable, but a date ($d) is returned for '$variable' variable.")
    }
  }

  def getDate(variable: String): LocalDate = {
    vars(variable) match {
      case d: LocalDate => d
      case n => throw new IllegalArgumentException(s"Expected a date variable, but a number ($n) is returned for '$variable' variable.")
    }
  }

  def getAny(variable: String): Any = {
    vars(variable)
  }

}
