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

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

class SqlGeneratorMySQL(sqlConfig: SqlConfig) extends SqlGeneratorBase(sqlConfig) {
  private val dateFormatterApp = DateTimeFormatter.ofPattern(sqlConfig.dateFormatApp)

  override val beginEndEscapeChars: (Char, Char) = ('`', '`')

  override def getDtable(sql: String): String = {
    if (sql.exists(_ == ' ')) {
      s"($sql) t"
    } else {
      sql
    }
  }

  override def getCountQuery(tableName: String): String = {
    s"SELECT COUNT(*) FROM ${escape(tableName)}"
  }

  override def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT COUNT(*) FROM ${escape(tableName)} WHERE $where"
  }

  override def getDataQuery(tableName: String, columns: Seq[String], limit: Option[Int]): String = {
    s"SELECT ${columnExpr(columns)} FROM ${escape(tableName)}${getLimit(limit)}"
  }

  override def getCountQueryForSql(filteredSql: String): String = {
    s"SELECT COUNT(*) FROM ($filteredSql) query"
  }

  override def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String], limit: Option[Int]): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT ${columnExpr(columns)} FROM ${escape(tableName)} WHERE $where${getLimit(limit)}"
  }

  override def getWhere(dateBegin: LocalDate, dateEnd: LocalDate): String = {
    if (sqlConfig.infoDateType == SqlColumnType.DATETIME) {
      s"$infoDateColumn >= '$dateBegin 00:00:00' AND $infoDateColumn < '${dateEnd.plusDays(1)} 00:00:00'"
    } else {
      val dateBeginLit = getDateLiteral(dateBegin)
      val dateEndLit = getDateLiteral(dateEnd)

      if (dateBeginLit == dateEndLit) {
        s"$infoDateColumn = $dateBeginLit"
      } else {
        s"$infoDateColumn >= $dateBeginLit AND $infoDateColumn <= $dateEndLit"
      }
    }
  }

  override def getDateLiteral(date: LocalDate): String = {
    sqlConfig.infoDateType match {
      case SqlColumnType.DATE =>
        val dateStr = DateTimeFormatter.ISO_LOCAL_DATE.format(date)
        s"'$dateStr'"
      case SqlColumnType.DATETIME =>
        val dateStr = DateTimeFormatter.ISO_LOCAL_DATE.format(date)
        s"'$dateStr'"
      case SqlColumnType.STRING =>
        val dateStr = dateFormatterApp.format(date)
        s"'$dateStr'"
      case SqlColumnType.NUMBER =>
        val dateStr = dateFormatterApp.format(date)
        s"$dateStr"
    }
  }


  override def getOffsetWhereCondition(column: String, condition: String, offset: OffsetValue): String = {
    offset match {
      case OffsetValue.DateTimeValue(ts) =>
        val ldt = LocalDateTime.ofInstant(ts, sqlConfig.serverTimeZone)
        val tsLiteral = timestampGenericDbFormatter.format(ldt)
        s"$column $condition '$tsLiteral'"
      case OffsetValue.IntegralValue(value) =>
        s"$column $condition $value"
      case OffsetValue.StringValue(value) =>
        s"$column $condition '$value'"
    }
  }

  private def getLimit(limit: Option[Int]): String = {
    limit.map(n => s" LIMIT $n").getOrElse("")
  }
}
