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
import za.co.absa.pramen.api.sql.{SqlColumnType, SqlConfig, SqlGeneratorBase}
import za.co.absa.pramen.core.sql.dialects.SasDialect

import java.sql.{Connection, ResultSet}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer

object SqlGeneratorSas {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * This is required for Spark to be able to handle data that comes from Sas JDBC drivers
    */
  lazy val registerDialect: Boolean = {
    log.info(s"Registering SAS dialect...")
    JdbcDialects.registerDialect(SasDialect)
    true
  }
}

class SqlGeneratorSas(sqlConfig: SqlConfig) extends SqlGeneratorBase(sqlConfig) {
  private val dateFormatterApp = DateTimeFormatter.ofPattern(sqlConfig.dateFormatApp)

  override val beginEndEscapeChars: (Char, Char) = ('\'', '\'')

  private var connection: Connection = _

  SqlGeneratorSas.registerDialect

  override def requiresConnection: Boolean = true

  override def setConnection(connection: Connection): Unit = this.connection = connection

  override def getDtable(sql: String): String = {
    if (sql.exists(_ == ' ')) {
      s"($sql)"
    } else {
      sql
    }
  }

  def getCountQuery(tableName: String): String = {
    s"SELECT ${getAliasExpression("COUNT(*)", "cnt")} FROM ${escape(tableName)}"
  }

  def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT ${getAliasExpression("COUNT(*)", "cnt")} FROM ${escape(tableName)} WHERE $where"
  }

  def getDataQuery(tableName: String, columns: Seq[String], limit: Option[Int]): String = {
    s"SELECT ${getColumnSql(tableName, columns)} FROM ${escape(tableName)}${getLimit(limit, hasWhere = false)}"
  }

  override def getCountQueryForSql(filteredSql: String): String = {
    s"SELECT COUNT(*) FROM ($filteredSql) AS query"
  }

  def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String], limit: Option[Int]): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT ${getColumnSql(tableName, columns)} FROM ${escape(tableName)} WHERE $where${getLimit(limit, hasWhere = true)}"
  }

  private def getColumnSql(tableName: String, columns: Seq[String]): String = {
    if (columns.isEmpty) {
      getColumns(tableName).map(name => s"${escape(name)} '${escape(name)}'").mkString(", ")
    } else {
      columns.map(escape).mkString(", ")
    }
  }

  private def getColumns(tableName: String): Seq[String] = {
    if (Option(connection).isEmpty) {
      throw new IllegalStateException(s"SAS SQL Dialect needs an active JDBC connection, but it hasn't been provided.")
    }

    val rs = executeDirectSqlQuery(s"SELECT * FROM $tableName WHERE 0=1")

    val columnCount = rs.getMetaData.getColumnCount

    val columns = new ListBuffer[String]

    var i = 1
    while (i <= columnCount) {
      val columnName = rs.getMetaData.getColumnName(i)
      val columnLabel = rs.getMetaData.getColumnLabel(i)

      columns.append(if (columnName.isEmpty) columnLabel else columnName)

      i += 1
    }

    columns.toSeq
  }

  @throws[java.sql.SQLException]
  private def executeDirectSqlQuery(sql: String): ResultSet = {
    val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    statement.executeQuery(sql)
  }

  override def getWhere(dateBegin: LocalDate, dateEnd: LocalDate): String = {
    val dateBeginLit = getDateLiteral(dateBegin)
    val dateEndLit = getDateLiteral(dateEnd)

    if (dateBeginLit == dateEndLit) {
      s"$infoDateColumn = $dateBeginLit"
    } else {
      s"$infoDateColumn >= $dateBeginLit AND $infoDateColumn <= $dateEndLit"
    }
  }

  override def getAliasExpression(expression: String, alias: String): String = {
    s"$expression AS $alias '$alias'"
  }

  override def getDateLiteral(date: LocalDate): String = {
    sqlConfig.infoDateType match {
      case SqlColumnType.DATE =>
        val dateStr = DateTimeFormatter.ISO_LOCAL_DATE.format(date)
        s"date'$dateStr'"
      case SqlColumnType.DATETIME =>
        throw new UnsupportedOperationException("DATETIME support for SAS is not supported yet.")
      case SqlColumnType.STRING =>
        val dateStr = dateFormatterApp.format(date)
        s"'$dateStr'"
      case SqlColumnType.NUMBER =>
        val dateStr = dateFormatterApp.format(date)
        s"$dateStr"
    }
  }

  override final def quoteSingleIdentifier(identifier: String): String = {
    if (identifier.startsWith(".") && identifier.toLowerCase.endsWith("n")) {
      identifier
    } else {
      s"'$identifier'n"
    }
  }

  private def getLimit(limit: Option[Int], hasWhere: Boolean): String = {
    limit.map(n => s" LIMIT $n").getOrElse("")
  }
}
