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

class SqlGeneratorHsqlDb(sqlConfig: SqlConfig) extends SqlGeneratorBase(sqlConfig) {

  private val dateFormatterApp = DateTimeFormatter.ofPattern(sqlConfig.dateFormatApp)

  override def getDtable(sql: String): String = {
    if (sql.exists(_ == ' ')) {
      s"($sql) t"
    } else {
      sql
    }
  }

  override def getCountQuery(tableName: String): String = {
    s"SELECT COUNT(*) AS C1 FROM $tableName"
  }

  override def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT COUNT(*) AS C1 FROM $tableName WHERE $where"
  }

  override def getDataQuery(tableName: String, limit: Option[Int]): String = {
    s"SELECT $columnExpr FROM $tableName${getLimit(limit)}"
  }

  override def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, limit: Option[Int]): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT $columnExpr FROM $tableName WHERE $where${getLimit(limit)}"
  }

  private def getWhere(dateBegin: LocalDate, dateEnd: LocalDate): String = {
    val dateBeginLit = getDateLiteral(dateBegin)
    val dateEndLit = getDateLiteral(dateEnd)

    val dateTypes = Array(SqlColumnType.DATETIME)

    val infoDateColumnAdjusted =
      if (dateTypes.contains(sqlConfig.infoDateType)) {
        s"CAST(${sqlConfig.infoDateColumn} AS DATE)"
      } else {
        sqlConfig.infoDateColumn
      }

    if (dateBeginLit == dateEndLit) {
      s"$infoDateColumnAdjusted = $dateBeginLit"
    } else {
      s"$infoDateColumnAdjusted >= $dateBeginLit AND $infoDateColumnAdjusted <= $dateEndLit"
    }
  }

  private def getDateLiteral(date: LocalDate): String = {
    val dateStr = dateFormatterApp.format(date)

    sqlConfig.infoDateType match {
      case SqlColumnType.DATE => s"TO_DATE('$dateStr', '${sqlConfig.dateFormatSql}')"
      case SqlColumnType.DATETIME => s"TO_DATE('$dateStr', '${sqlConfig.dateFormatSql}')"
      case SqlColumnType.STRING => s"'$dateStr'"
      case SqlColumnType.NUMBER => s"$dateStr"
    }
  }

  private def getLimit(limit: Option[Int]): String = {
    limit.map(n => s" LIMIT $n").getOrElse("")
  }
}
