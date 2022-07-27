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

package za.co.absa.pramen.core.expr.parser

import za.co.absa.pramen.core.expr.exceptions.SyntaxErrorException

import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate}
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

class DateExpressionEvaluator(vars: Map[String, Any], expr: String) extends DateExprBuilder {
  val ops = new ListBuffer[String]
  val values = new ListBuffer[Any]

  override def openParen(pos: Int): Unit = ops += "("

  override def closeParen(pos: Int): Unit = {
    if (ops.isEmpty) {
      if (values.size != 1) {
        throw new SyntaxErrorException(s"Empty expression at $pos in '$expr'.")
      }
    } else {
      eval()
    }
  }

  override def addOperationPlus(pos: Int): Unit = ops += "+"

  override def addOperationMinus(pos: Int): Unit = ops += "-"

  override def addVariable(name: String, pos: Int): Unit = {
    if (!vars.contains(name)) {
      throw new SyntaxErrorException(s"Unset variable '$name' used.")
    } else {
      values += vars(name)
    }
    if (ops.nonEmpty && (ops.last == "+" || ops.last == "-")) {
      eval()
    }
  }

  override def addFunction(name: String, pos: Int): Unit = {
    ops += name
  }

  override def addNumLiteral(num: Int, pos: Int): Unit = {
    values += num
    if (ops.nonEmpty && (ops.last == "+" || ops.last == "-")) {
      eval()
    }
  }

  def addDateLiteral(date: LocalDate, pos: Int): Unit = {
    values += date
    if (ops.nonEmpty && (ops.last == "+" || ops.last == "-")) {
      eval()
    }
  }

  def getResult: Any = {
    while (ops.nonEmpty) {
      eval()
    }
    if (values.isEmpty) {
      throw new SyntaxErrorException(s"Empty expressions are not supported in '$expr'.")
    } else if (values.size > 1) {
      throw new SyntaxErrorException(s"Malformed expression: '$expr'.")
    } else {
      values.head match {
        case d: LocalDate => d
        case n: Int => n
        case s: String => s
        case n => throw new SyntaxErrorException(s"Unexpected type of '$n' which is the result of: '$expr'.")
      }
    }
  }

  @tailrec
  private def eval(): Unit = {
    val op = ops.last
    ops.remove(ops.size - 1)

    op match {
      case "(" => if (ops.nonEmpty && ops.last != "(") eval()
      case "+" =>
        expectArguments(2)
        val b = getDateOrInt
        val a = getDateOrInt
        val r = (a, b) match {
          case (n1: Int, n2: Int) => n1 + n2
          case (n: Int, d: LocalDate) => d.plusDays(n)
          case (d: LocalDate, n: Int) => d.plusDays(n)
          case (_: LocalDate, _: LocalDate) => throw new SyntaxErrorException(s"Cannot add 2 dates.")
        }
        values += r
      case "-" =>
        expectArguments(2)
        val b = getDateOrInt
        val a = getDateOrInt
        val r = (a, b) match {
          case (n1: Int, n2: Int) => n1 - n2
          case (n: Int, d: LocalDate) => d.minusDays(n)
          case (d: LocalDate, n: Int) => d.minusDays(n)
          case (_: LocalDate, _: LocalDate) => throw new SyntaxErrorException(s"Cannot subtract 2 dates.")
        }
        values += r
      case "monthOf" =>
        expectArguments(1)
        val d = getDate
        val r = d.getMonthValue
        values += r
      case "yearOf" =>
        expectArguments(1)
        val d = getDate
        val r = d.getYear
        values += r
      case "yearMonthOf" =>
        expectArguments(1)
        val d = getDate
        val r = d.format(DateTimeFormatter.ofPattern("yyyy-MM"))
        values += r
      case "dayOfMonth" =>
        expectArguments(1)
        val d = getDate
        val r = d.getDayOfMonth
        values += r
      case "dayOfWeek" =>
        expectArguments(1)
        val d = getDate
        val r = d.getDayOfWeek.getValue
        values += r
      case "plusDays" =>
        expectArguments(2)
        val m = getInt
        val d = getDate
        val r = d.plusDays(m)
        values += r
      case "minusDays" =>
        expectArguments(2)
        val m = getInt
        val d = getDate
        val r = d.minusDays(m)
        values += r
      case "plusWeeks" =>
        expectArguments(2)
        val m = getInt
        val d = getDate
        val r = d.plusWeeks(m)
        values += r
      case "minusWeeks" =>
        expectArguments(2)
        val m = getInt
        val d = getDate
        val r = d.minusWeeks(m)
        values += r
      case "plusMonths" =>
        expectArguments(2)
        val m = getInt
        val d = getDate
        val r = d.plusMonths(m)
        values += r
      case "minusMonths" =>
        expectArguments(2)
        val m = getInt
        val d = getDate
        val r = d.minusMonths(m)
        values += r
      case "beginOfMonth" =>
        expectArguments(1)
        val d = getDate
        val r = LocalDate.of(d.getYear, d.getMonth, 1)
        values += r
      case "endOfMonth" =>
        expectArguments(1)
        val d = getDate
        val dPlusMonth = d.plusMonths(1)
        val r = LocalDate.of(dPlusMonth.getYear, dPlusMonth.getMonth, 1)
          .minusDays(1)
        values += r
      case "lastDayOfMonth" =>
        expectArguments(2)
        val n = getInt
        val d = getDate
        values += getLastDayOfMonth(d, n)
      case "lastMonday" =>
        expectArguments(1)
        values += getLastWeekDay(getDate, DayOfWeek.MONDAY)
      case "lastTuesday" =>
        expectArguments(1)
        values += getLastWeekDay(getDate, DayOfWeek.TUESDAY)
      case "lastWednesday" =>
        expectArguments(1)
        values += getLastWeekDay(getDate, DayOfWeek.WEDNESDAY)
      case "lastThursday" =>
        expectArguments(1)
        values += getLastWeekDay(getDate, DayOfWeek.THURSDAY)
      case "lastFriday" =>
        expectArguments(1)
        values += getLastWeekDay(getDate, DayOfWeek.FRIDAY)
      case "lastSaturday" =>
        expectArguments(1)
        values += getLastWeekDay(getDate, DayOfWeek.SATURDAY)
      case "lastSunday" =>
        expectArguments(1)
        values += getLastWeekDay(getDate, DayOfWeek.SUNDAY)
      case f => throw new SyntaxErrorException(s"Unsupported function '$f' in '$expr'.")
    }
  }

  private def getLastWeekDay(day: LocalDate, dayOfWeek: DayOfWeek): LocalDate = {
    var r = day
    while (r.getDayOfWeek != dayOfWeek) {
      r = r.minusDays(1)
    }
    r
  }

  private def getLastDayOfMonth(day: LocalDate, dayOfMonth: Int): LocalDate = {
    var r = day
    while (r.getDayOfMonth != dayOfMonth) {
      r = r.minusDays(1)
    }
    r
  }

  private def expectArguments(n: Int): Unit = {
    if (values.size < n)
      throw new SyntaxErrorException(s"Expected more arguments in '$expr'.")
  }

  private def getDateOrInt: Any = {
    val a = values.last
    values.remove(values.size - 1)
    a match {
      case n: Int => n
      case d: LocalDate => d
      case x => throw new SyntaxErrorException(s"Unexpected type value of '$x' in '$expr'.")
    }
  }

  private def getInt: Int = {
    val a = values.last
    values.remove(values.size - 1)
    a match {
      case n: Int => n
      case x => throw new SyntaxErrorException(s"Expected a number, got $x in '$expr'.")
    }
  }

  private def getDate: LocalDate = {
    val a = values.last
    values.remove(values.size - 1)
    a match {
      case d: LocalDate => d
      case x => throw new SyntaxErrorException(s"Expected a date, got $x in '$expr'.")
    }
  }

}
