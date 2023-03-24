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

import com.typesafe.config.Config

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class SqlGeneratorOracle(sqlConfig: SqlConfig, extraConfig: Config) extends SqlGeneratorBase(sqlConfig) {
  val ORACLE_DATE_PATTERN = "yyyy-MM-dd"

  private val dateFormatterApp = DateTimeFormatter.ofPattern(sqlConfig.dateFormatApp)
  private val dateFormatterOracle = DateTimeFormatter.ofPattern(ORACLE_DATE_PATTERN)

  override def getDtable(sql: String): String = {
    if (sql.exists(_ == ' ')) {
      s"($sql)"
    } else {
      sql
    }
  }

  def getCountQuery(tableName: String): String = {
    s"SELECT COUNT(*) FROM $tableName"
  }

  def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT COUNT(*) FROM $tableName WHERE $where"
  }

  def getDataQuery(tableName: String, columns: Seq[String], limit: Option[Int]): String = {
    s"SELECT ${columnExpr(columns)} FROM $tableName${getLimit(limit, hasWhere = false)}"
  }

  def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String], limit: Option[Int]): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT ${columnExpr(columns)} FROM $tableName WHERE $where${getLimit(limit, hasWhere = true)}"
  }

  private def getWhere(dateBegin: LocalDate, dateEnd: LocalDate): String = {
    val dateBeginLit = getDateLiteral(dateBegin)
    val dateEndLit = getDateLiteral(dateEnd)

    val infoDateColumn = sqlConfig.infoDateColumn

    if (dateBeginLit == dateEndLit) {
      s"$infoDateColumn = $dateBeginLit"
    } else {
      s"$infoDateColumn >= $dateBeginLit AND $infoDateColumn <= $dateEndLit"
    }
  }

  private def getDateLiteral(date: LocalDate): String = {
    sqlConfig.infoDateType match {
      case SqlColumnType.DATE => {
        val dateStr = dateFormatterOracle.format(date)
        s"date'$dateStr'"
      }
      case SqlColumnType.DATETIME => throw new NotImplementedError("DATETIME support for Denodo is not supported yet.")
      case SqlColumnType.STRING => {
        val dateStr = dateFormatterApp.format(date)
        s"'$dateStr'"
      }
      case SqlColumnType.NUMBER => {
        val dateStr = dateFormatterApp.format(date)
        s"$dateStr"
      }
    }
  }

  private def getLimit(limit: Option[Int], hasWhere: Boolean): String = {
    if (hasWhere) {
      limit.map(n => s" AND ROWNUM <= $n").getOrElse("")
    } else {
      limit.map(n => s" WHERE ROWNUM <= $n").getOrElse("")
    }
  }

}
