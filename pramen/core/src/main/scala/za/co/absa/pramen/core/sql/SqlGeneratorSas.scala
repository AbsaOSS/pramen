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
import za.co.absa.pramen.framework.sql.impl.SasDialect

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

class SqlGeneratorSas(sqlConfig: SqlConfig, extraConfig: Config) extends SqlGeneratorBase(sqlConfig) {
  val ORACLE_DATE_PATTERN = "yyyy-MM-dd"

  private val dateFormatterApp = DateTimeFormatter.ofPattern(sqlConfig.dateFormatApp)
  private val dateFormatterOracle = DateTimeFormatter.ofPattern(ORACLE_DATE_PATTERN)

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
    s"SELECT COUNT(*) AS cnt 'cnt' FROM $tableName"
  }

  def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT COUNT(*) AS cnt 'cnt' FROM $tableName WHERE $where"
  }

  def getDataQuery(tableName: String, limit: Option[Int]): String = {
    s"SELECT ${getColumnSql(tableName)} FROM $tableName${getLimit(limit, hasWhere = false)}"
  }

  def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, limit: Option[Int]): String = {
    val where = getWhere(infoDateBegin, infoDateEnd)
    s"SELECT ${getColumnSql(tableName)} FROM $tableName WHERE $where${getLimit(limit, hasWhere = true)}"
  }

  private def getColumnSql(tableName: String): String = {
    if (sqlConfig.columns.isEmpty) {
      getColumns(tableName).map(name => s"$name '$name'").mkString(", ")
    } else {
      sqlConfig.columns.mkString(", ")
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

    columns
  }

  @throws[java.sql.SQLException]
  private def executeDirectSqlQuery(sql: String): ResultSet = {
    val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    statement.executeQuery(sql)
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
      case SqlColumnType.DATE   =>
        val dateStr = dateFormatterOracle.format(date)
        s"date'$dateStr'"
      case SqlColumnType.DATETIME => throw new NotImplementedError("DATETIME support for Denodo is not supported yet.")
      case SqlColumnType.STRING =>
        val dateStr = dateFormatterApp.format(date)
        s"'$dateStr'"
      case SqlColumnType.NUMBER =>
        val dateStr = dateFormatterApp.format(date)
        s"$dateStr"
    }
  }

  private def getLimit(limit: Option[Int], hasWhere: Boolean): String = {
    limit.map(n => s" LIMIT $n").getOrElse("")
  }

}
