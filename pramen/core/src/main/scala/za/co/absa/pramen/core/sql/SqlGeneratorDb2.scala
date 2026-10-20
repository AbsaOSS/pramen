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

import za.co.absa.pramen.api.offset.OffsetValue
import za.co.absa.pramen.api.sql.{SqlColumnType, SqlConfig, SqlGeneratorBase}

import java.sql.Connection
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId, ZoneOffset}
import scala.util.Try

class SqlGeneratorDb2(sqlConfig: SqlConfig) extends SqlGeneratorBase(sqlConfig) {
  private val dateFormatterApp = DateTimeFormatter.ofPattern(sqlConfig.dateFormatApp)

  override val beginEndEscapeChars: (Char, Char) = ('\"', '\"')

  override def getDtable(sql: String): String = {
    if (sql.exists(_ == ' ')) {
      getAliasExpression(s"($sql)", "T")
    } else {
      sql
    }
  }

  override def getCountQuery(tableName: String): String = {
    s"SELECT ${getAliasExpression("COUNT(*)", "CNT")} FROM ${escape(tableName)}"
  }

  override def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT ${getAliasExpression("COUNT(*)", "CNT")} FROM ${escape(tableName)} WHERE $where"
  }

  override def getCountQueryForSql(filteredSql: String): String = {
    s"SELECT COUNT(*) FROM ($filteredSql) AS query"
  }

  override def getDataQuery(tableName: String, columns: Seq[String], limit: Option[Int]): String = {
    s"SELECT ${columnExpr(columns)} FROM ${escape(tableName)}${getLimit(limit)}"
  }

  override def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String], limit: Option[Int]): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT ${columnExpr(columns)} FROM ${escape(tableName)} WHERE $where${getLimit(limit)}"
  }

  override def getServerTimezoneQuery: String = "SELECT CURRENT TIMEZONE FROM SYSIBM.SYSDUMMY1"

  override def getServerTimezone(connection: Connection): Try[ZoneId] = {
    Try {
      val query = getServerTimezoneQuery
      val statement = connection.createStatement()
      try {
        val resultSet = statement.executeQuery(query)
        try {
          if (resultSet.next()) {
            val timezoneRaw = resultSet.getBigDecimal(1).intValue()
            SqlGeneratorDb2.convertDb2TzOffsets(timezoneRaw)
          } else {
            throw new IllegalStateException(s"The server timezone query returned no rows: $query")
          }
        } finally {
          resultSet.close()
        }
      } finally {
        statement.close()
      }
    }
  }

  override def getWhere(dateBegin: LocalDate, dateEnd: LocalDate): String = {
    val dateBeginLit = getDateLiteral(dateBegin)
    val dateEndLit = getDateLiteral(dateEnd)

    val dateTypes: Array[SqlColumnType] = Array(SqlColumnType.DATETIME)

    val infoDateColumnAdjusted =
      if (dateTypes.contains(sqlConfig.infoDateType)) {
        s"CAST($infoDateColumn AS DATE)"
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
        s"DATE '$dateStr'"
      case SqlColumnType.DATETIME =>
        val dateStr = DateTimeFormatter.ISO_LOCAL_DATE.format(date)
        s"DATE '$dateStr'"
      case SqlColumnType.STRING =>
        val dateStr = dateFormatterApp.format(date)
        s"'$dateStr'"
      case SqlColumnType.NUMBER =>
        val dateStr = dateFormatterApp.format(date)
        s"$dateStr"
    }
  }

  override def getOffsetWhereCondition(column: String, condition: String, offset: OffsetValue, serverTimeZone: Option[ZoneId]): String = {
    offset match {
      case OffsetValue.DateTimeValue(ts) =>
        val tsLiteral = getGenericTimestampString(ts, serverTimeZone)
        s"$column $condition TIMESTAMP('$tsLiteral')"
      case OffsetValue.IntegralValue(value) =>
        s"$column $condition $value"
      case OffsetValue.StringValue(value) =>
        s"$column $condition '$value'"
      case t =>
        throw new IllegalArgumentException(s"Offset type [${t.dataType.dataTypeString}] is not supported in DB2 SQL query generator.")
    }
  }

  private def getLimit(limit: Option[Int]): String = {
    limit.map(n => s" LIMIT $n").getOrElse("")
  }
}

object SqlGeneratorDb2 {
  /**
    * Converts a DB2 timezone offset value into a Java `ZoneId`.
    *
    *  DB2 returns timezone as ±hhmmss
    *  Examples:
    *    20000  -> +02:00:00
    *   -53000  -> -05:30:00
    *
    * @param db2Offset the timezone offset as returned by DB2, encoded as a signed integer in the `hhmmss` format
    * @return a `ZoneOffset` (as a `ZoneId`) representing the same offset from UTC
    * @throws java.time.DateTimeException if the decoded hours, minutes or seconds are outside the range supported by `ZoneOffset`
    */
  def convertDb2TzOffsets(db2Offset: Int): ZoneId = {
    val sign = if (db2Offset < 0) -1 else 1
    val abs = math.abs(db2Offset)

    val hours = abs / 10000
    val minutes = (abs % 10000) / 100
    val seconds = abs % 100

    ZoneOffset.ofHoursMinutesSeconds(
      sign * hours,
      sign * minutes,
      sign * seconds
    )
  }
}