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

package za.co.absa.pramen.core.sql

import za.co.absa.pramen.api.offset.{OffsetInfo, OffsetValue}
import za.co.absa.pramen.api.sql.SqlGeneratorBase.{needsEscaping, validateIdentifier, validateOffsetValue}
import za.co.absa.pramen.api.sql.{SqlColumnType, SqlConfig, SqlGenerator}
import za.co.absa.pramen.core.utils.MutableStack

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.collection.mutable.ListBuffer

class SqlGeneratorMicrosoft(sqlConfig: SqlConfig) extends SqlGenerator {
  private val dateFormatterApp = DateTimeFormatter.ofPattern(sqlConfig.dateFormatApp)
  private val timestampMsDbFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
  private val isIso = sqlConfig.dateFormatApp.toLowerCase.startsWith("yyyy-mm-dd")

  // 23 is "yyyy-MM-dd", see https://www.mssqltips.com/sqlservertip/1145/date-and-time-conversions-using-sql-server/
  private val isoFormatMsSqlRef = 23

  val beginEndEscapeChars: (Char, Char) = ('[', ']')
  val escapeChar2 = '\"'

  override def getDtable(sql: String): String = {
    if (sql.exists(_ == ' ')) {
      getAliasExpression(s"($sql)", "tbl")
    } else {
      sql
    }
  }

  def getCountQuery(tableName: String): String = {
    s"SELECT ${getAliasExpression("COUNT(*)", "CNT")} FROM ${escape(tableName)} WITH (NOLOCK)"
  }

  def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT ${getAliasExpression("COUNT(*)", "CNT")} FROM ${escape(tableName)} WITH (NOLOCK) WHERE $where"
  }

  override def getCountQueryForSql(filteredSql: String): String = {
    s"SELECT COUNT(*) FROM ($filteredSql) AS query"
  }

  override def getDataQuery(tableName: String, columns: Seq[String], limit: Option[Int]): String = {
    s"SELECT ${getLimit(limit)}${columnExpr(columns)} FROM ${escape(tableName)} WITH (NOLOCK)"
  }

  override def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String], limit: Option[Int]): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT ${getLimit(limit)}${columnExpr(columns)} FROM ${escape(tableName)} WITH (NOLOCK) WHERE $where"
  }

  override def getWhere(dateBegin: LocalDate, dateEnd: LocalDate): String = {
    val dateBeginLit = getDateLiteral(dateBegin)
    val dateEndLit = getDateLiteral(dateEnd)

    val infoDateColumnAdjusted = if (sqlConfig.infoDateType == SqlColumnType.DATETIME) {
      s"CONVERT(DATE, $infoDateColumn, $isoFormatMsSqlRef)"
    } else if (sqlConfig.infoDateType == SqlColumnType.STRING && isIso) {
      s"TRY_CONVERT(DATE, $infoDateColumn, $isoFormatMsSqlRef)"
    } else {
      infoDateColumn
    }

    if (dateBeginLit == dateEndLit) {
      s"$infoDateColumnAdjusted = $dateBeginLit"
    } else {
      s"$infoDateColumnAdjusted >= $dateBeginLit AND $infoDateColumnAdjusted <= $dateEndLit"
    }
  }

  override def getDateLiteral(date: LocalDate): String = {
    sqlConfig.infoDateType match {
      case SqlColumnType.DATE =>
        val dateStr = DateTimeFormatter.ISO_LOCAL_DATE.format(date)
        s"CONVERT(DATE, '$dateStr', $isoFormatMsSqlRef)"
      case SqlColumnType.DATETIME =>
        val dateStr = DateTimeFormatter.ISO_LOCAL_DATE.format(date)
        s"CONVERT(DATE, '$dateStr', $isoFormatMsSqlRef)"
      case SqlColumnType.STRING =>
        if (isIso) {
          val dateStr = DateTimeFormatter.ISO_LOCAL_DATE.format(date)
          s"CONVERT(DATE, '$dateStr', $isoFormatMsSqlRef)"
        } else {
          val dateStr = dateFormatterApp.format(date)
          s"'$dateStr'"
        }
      case SqlColumnType.NUMBER =>
        val dateStr = dateFormatterApp.format(date)
        s"$dateStr"
    }
  }

  override def getAliasExpression(expression: String, alias: String): String = {
    s"$expression AS ${escape(alias)}"
  }

  override def quote(identifier: String): String = {
    validateIdentifier(identifier)
    splitComplexIdentifier(identifier).map(quoteSingleIdentifier).mkString(".")
  }

  override def unquote(identifier: String): String = {
    validateIdentifier(identifier)
    splitComplexIdentifier(identifier).map(unquoteSingleIdentifier).mkString(".")
  }

  override def escape(identifier: String): String = {
    if (needsEscaping(sqlConfig.identifierQuotingPolicy, identifier)) {
      quote(identifier)
    } else {
      identifier
    }
  }

  override def getDataQueryIncremental(tableName: String,
                                       onlyForInfoDate: Option[LocalDate],
                                       offsetFromOpt: Option[OffsetValue],
                                       offsetToOpt: Option[OffsetValue],
                                       columns: Seq[String]): String = {
    if (sqlConfig.offsetInfo.isEmpty)
      throw new IllegalArgumentException(s"Offset information is not configured for database table: $tableName.")

    val dataQuery = onlyForInfoDate match {
      case Some(infoDate) => getDataQuery(tableName, infoDate, infoDate, columns, None)
      case None => getDataQuery(tableName, columns, None)
    }

    val offsetWhere = getOffsetWhereClause(sqlConfig.offsetInfo.get, offsetFromOpt, offsetToOpt)

    if (offsetWhere.nonEmpty) {
      if (onlyForInfoDate.isEmpty) {
        s"$dataQuery WHERE $offsetWhere"
      } else {
        s"$dataQuery AND $offsetWhere"
      }
    } else {
      dataQuery
    }
  }

  private[core] def getOffsetWhereClause(offsetInfo: OffsetInfo, offsetFromOpt: Option[OffsetValue], offsetToOpt: Option[OffsetValue]): String = {
    val offsetColumn = escape(offsetInfo.offsetColumn)

    (offsetFromOpt, offsetToOpt) match {
      case (Some(offsetFrom), Some(offsetTo)) =>
        validateOffsetValue(offsetFrom)
        validateOffsetValue(offsetTo)
        s"${getOffsetWhereCondition(offsetColumn, ">=", offsetFrom)} AND ${getOffsetWhereCondition(offsetColumn, "<=", offsetTo)}"
      case (Some(offsetFrom), None) =>
        validateOffsetValue(offsetFrom)
        s"${getOffsetWhereCondition(offsetColumn, ">", offsetFrom)}"
      case (None, Some(offsetTo)) =>
        validateOffsetValue(offsetTo)
        s"${getOffsetWhereCondition(offsetColumn, "<=", offsetTo)}"
      case (None, None) =>
        ""
    }
  }

  private[core] def getOffsetWhereCondition(column: String, condition: String, offset: OffsetValue): String = {
    offset match {
      case OffsetValue.DateTimeValue(ts) =>
        val ldt = LocalDateTime.ofInstant(ts, sqlConfig.serverTimeZone)
        val tsLiteral = timestampMsDbFormatter.format(ldt)
        s"$column $condition '$tsLiteral'"
      case OffsetValue.IntegralValue(value) =>
        s"$column $condition $value"
      case OffsetValue.StringValue(value) =>
        s"$column $condition '$value'"
    }
  }

  private def getLimit(limit: Option[Int]): String = {
    limit.map(n => s"TOP $n ").getOrElse("")
  }

  private def columnExpr(columns: Seq[String]): String = {
    if (columns.isEmpty) {
      "*"
    } else {
      columns.map(col => escape(col)).mkString(", ")
    }
  }

  private def infoDateColumn: String = {
    escape(sqlConfig.infoDateColumn)
  }

  private def unquoteSingleIdentifier(identifier: String): String = {
    val (escapeBegin, escapeEnd) = beginEndEscapeChars

    if (identifier.startsWith(s"$escapeBegin") && identifier.endsWith(s"$escapeEnd") && identifier.length > 2) {
      identifier.substring(1, identifier.length - 1)
    } else if (identifier.startsWith(s"$escapeChar2") && identifier.endsWith(s"$escapeChar2") && identifier.length > 2) {
      identifier.substring(1, identifier.length - 1)
    } else {
      identifier
    }
  }

  private def quoteSingleIdentifier(identifier: String): String = {
    val (escapeBegin, escapeEnd) = beginEndEscapeChars

    if (
      (identifier.startsWith(s"$escapeBegin") && identifier.endsWith(s"$escapeEnd")) ||
        (identifier.startsWith(s"$escapeChar2") && identifier.endsWith(s"$escapeChar2"))
    ) {
      identifier
    } else {
      s"$escapeBegin$identifier$escapeEnd"
    }
  }

  private[core] def splitComplexIdentifier(identifier: String): Seq[String] = {
    val trimmedIdentifier = identifier.trim

    if (trimmedIdentifier.isEmpty) {
      throw new IllegalArgumentException(f"Found an empty table name or column name ('$identifier').")
    }

    val (escapeBegin1, escapeEnd1) = beginEndEscapeChars

    val output = new ListBuffer[String]
    val curColumn = new StringBuffer()
    val len = trimmedIdentifier.length
    val nestingChar = new MutableStack[Char]
    var i = 0

    while (i < len) {
      val c = trimmedIdentifier(i)
      val nextChar = if (i == len - 1) ' ' else trimmedIdentifier(i + 1)

      if (nestingChar.isEmpty && c == '.') {
        output += curColumn.toString
        curColumn.setLength(0)
      } else {
        curColumn.append(c)
      }

      if (c == escapeChar2) {
        if (curColumn.length() > 1 && i < len - 1 && nextChar != '.')
          throw new IllegalArgumentException(f"Invalid character '$escapeChar2' in the identifier '$identifier', position $i.")
        nestingChar.pop() match {
          case Some(ch) =>
            if (ch != escapeChar2)
              throw new IllegalArgumentException(f"Invalid character '$escapeChar2' in the identifier '$identifier', position $i.")
          case None =>
            nestingChar.push(escapeChar2)
        }
      } else if (c == escapeBegin1) {
        if (nestingChar.nonEmpty && nestingChar.peek() == escapeChar2) {
          throw new IllegalArgumentException(f"Invalid character '$escapeChar2' in the identifier '$identifier', position $i.")
        }
        if (curColumn.length() > 1 && i < len - 1 && nextChar != '.')
          throw new IllegalArgumentException(f"Invalid character '$escapeBegin1' in the identifier '$identifier', position $i.")
        nestingChar.push(escapeBegin1)
      } else if (c == escapeEnd1) {
        nestingChar.pop() match {
          case Some(ch) =>
            if (ch == escapeChar2)
              throw new IllegalArgumentException(f"Found not matching '$escapeChar2' in the identifier '$identifier'.")
          case None =>
            throw new IllegalArgumentException(f"Found not matching '$escapeEnd1' in the identifier '$identifier'.")
        }
      }
      i += 1
    }

    nestingChar.pop().foreach{ch =>
      throw new IllegalArgumentException(f"Found not matching '$ch' in the identifier '$identifier'.")
    }

    if (curColumn.toString.nonEmpty)
      output += curColumn.toString

    output.toSeq
  }

}
