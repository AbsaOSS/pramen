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

import org.apache.spark.sql.jdbc.JdbcDialects
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.offset.OffsetValue
import za.co.absa.pramen.api.sql.{SqlColumnType, SqlConfig, SqlGeneratorBase}
import za.co.absa.pramen.core.sql.dialects.DenodoDialect

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

object SqlGeneratorDenodo {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * This is required for Spark to be able to handle data that comes from Denodo JDBC drivers
    */
  lazy val registerDialect: Boolean = {
    log.info(s"Registering Denodo dialect...")
    JdbcDialects.registerDialect(DenodoDialect)
    true
  }
}

class SqlGeneratorDenodo(sqlConfig: SqlConfig) extends SqlGeneratorBase(sqlConfig) {
  private val dateFormatterApp = DateTimeFormatter.ofPattern(sqlConfig.dateFormatApp)

  SqlGeneratorDenodo.registerDialect

  override val beginEndEscapeChars: (Char, Char) = ('\"', '\"')

  override def getDtable(sql: String): String = {
    if (sql.exists(_ == ' ')) {
      s"($sql) tbl"
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

  override def getOffsetWhereCondition(column: String, condition: String, offset: OffsetValue): String = {
    offset match {
      case OffsetValue.DateTimeValue(ts) =>
        val ldt = LocalDateTime.ofInstant(ts, sqlConfig.serverTimeZone)
        val tsLiteral = timestampGenericDbFormatter.format(ldt)
        s"$column $condition TIMESTAMP '$tsLiteral'"
      case OffsetValue.IntegralValue(value) =>
        s"$column $condition $value"
      case OffsetValue.StringValue(value) =>
        s"$column $condition '$value'"
    }
  }

  private def getLimit(limit: Option[Int]): String = {
    // limit.map(n => s" LIMIT $n").getOrElse("")
    // Adding limits to Denodo queries seems is not supported
    ""
  }
}
