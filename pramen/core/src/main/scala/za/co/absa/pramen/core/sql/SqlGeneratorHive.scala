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

package za.co.absa.pramen.framework.sql

import com.typesafe.config.Config
import org.apache.spark.sql.jdbc.JdbcDialects
import org.slf4j.LoggerFactory
import za.co.absa.pramen.framework.sql.impl.HiveDialect

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object SqlGeneratorHive {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * This is required for Spark to be able to handle data that comes from Hive JDBC drivers
    */
  lazy val registerDialect: Boolean = {
    log.info(s"Registering Hive dialect...")
    JdbcDialects.registerDialect(HiveDialect)
    true
  }
}

class SqlGeneratorHive(sqlConfig: SqlConfig, extraConfig: Config) extends SqlGeneratorBase(sqlConfig) {

  private val dateFormatterApp = DateTimeFormatter.ofPattern(sqlConfig.dateFormatApp)

  SqlGeneratorHive.registerDialect

  override def getDtable(sql: String): String = {
    s"($sql) tbl"
  }

  override def getCountQuery(tableName: String): String = {
    s"SELECT COUNT(*) FROM $tableName"
  }

  override def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT COUNT(*) FROM $tableName WHERE $where"
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

    val infoDateColumn = sqlConfig.infoDateColumn

    if (dateBeginLit == dateEndLit) {
      s"$infoDateColumn = $dateBeginLit"
    } else {
      s"$infoDateColumn >= $dateBeginLit AND $infoDateColumn <= $dateEndLit"
    }
  }

  private def getDateLiteral(date: LocalDate): String = {
    val dateStr = dateFormatterApp.format(date)

    sqlConfig.infoDateType match {
      case SqlColumnType.DATE => s"to_date('$dateStr')"
      case SqlColumnType.DATETIME => throw new NotImplementedError("DATETIME support for Denodo is not supported yet.")
      case SqlColumnType.STRING => s"'$dateStr'"
      case SqlColumnType.NUMBER => s"$dateStr"
    }
  }

  private def getLimit(limit: Option[Int]): String = {
    limit.map(n => s" LIMIT $n").getOrElse("")
  }
}
