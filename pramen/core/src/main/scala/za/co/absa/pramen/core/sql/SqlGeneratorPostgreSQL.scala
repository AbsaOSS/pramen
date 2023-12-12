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

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class SqlGeneratorPostgreSQL(sqlConfig: SqlConfig) extends SqlGeneratorBase(sqlConfig) {
  private val dateFormatterApp = DateTimeFormatter.ofPattern(sqlConfig.dateFormatApp)

  override def getDtable(sql: String): String = {
    if (sql.exists(_ == ' ')) {
      s"($sql) t"
    } else {
      sql
    }
  }

  override def getCountQuery(tableName: String): String = {
    s"SELECT COUNT(*) FROM ${escapeIdentifier(tableName)}"
  }

  override def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT COUNT(*) FROM ${escapeIdentifier(tableName)} WHERE $where"
  }

  override def getDataQuery(tableName: String, columns: Seq[String], limit: Option[Int]): String = {
    s"SELECT ${columnExpr(columns)} FROM ${escapeIdentifier(tableName)}${getLimit(limit)}"
  }

  override def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String], limit: Option[Int]): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT ${columnExpr(columns)} FROM ${escapeIdentifier(tableName)} WHERE $where${getLimit(limit)}"
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
        s"date'$dateStr'"
      case SqlColumnType.DATETIME =>
        val dateStr = DateTimeFormatter.ISO_LOCAL_DATE.format(date)
        s"date'$dateStr'"
      case SqlColumnType.STRING =>
        val dateStr = dateFormatterApp.format(date)
        s"'$dateStr'"
      case SqlColumnType.NUMBER =>
        val dateStr = dateFormatterApp.format(date)
        s"$dateStr"
    }
  }

  override final def wrapIdentifier(identifier: String): String = {
    if (identifier.startsWith("\"") && identifier.endsWith("\"")) {
      identifier
    } else {
      val q = "\""
      s"$q$identifier$q"
    }
  }

  private def getLimit(limit: Option[Int]): String = {
    limit.map(n => s" LIMIT $n").getOrElse("")
  }
}
