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
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.time.LocalDate

trait SqlGenerator {
  def getDtable(sql: String): String

  def getCountQuery(tableName: String): String

  def getCountQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): String

  def getDataQuery(tableName: String, columns: Seq[String], limit: Option[Int]): String

  def getDataQuery(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String], limit: Option[Int]): String

  def getWhere(dateBegin: LocalDate, dateEnd: LocalDate): String

  /** Returns the date literal for the dialect of the SQL. */
  def getDateLiteral(date: LocalDate): String

  /**
    * This quotes an identifier name with characters specific to SQL dialects.
    * If the identifier is already wrapped, it won't be double wrapped.
    * It supports partially quoted identifiers. E.g. '"my_catalog".my table' will be quoted as '"my_catalog"."my table"'.
    */
  def quote(identifier: String): String

  def requiresConnection: Boolean = false

  def setConnection(connection: Connection): Unit = {}
}

object SqlGenerator {
  private val log = LoggerFactory.getLogger(this.getClass)

  def fromDriverName(driver: String, sqlConfig: SqlConfig, extraConfig: Config): SqlGenerator = {
    val sqlGenerator = driver match {
      case "org.postgresql.Driver"                        => new SqlGeneratorPostgreSQL(sqlConfig)
      case "oracle.jdbc.OracleDriver"                     => new SqlGeneratorOracle(sqlConfig)
      case "net.sourceforge.jtds.jdbc.Driver"             => new SqlGeneratorMicrosoft(sqlConfig)
      case "com.microsoft.sqlserver.jdbc.SQLServerDriver" => new SqlGeneratorMicrosoft(sqlConfig)
      case "com.denodo.vdp.jdbc.Driver"                   => new SqlGeneratorDenodo(sqlConfig)
      case "com.sas.rio.MVADriver"                        => new SqlGeneratorSas(sqlConfig)
      case "com.cloudera.hive.jdbc41.HS2Driver"           => new SqlGeneratorHive(sqlConfig)
      case "com.simba.hive.jdbc41.HS2Driver"              => new SqlGeneratorHive(sqlConfig)
      case "com.simba.spark.jdbc.Driver"                  => new SqlGeneratorHive(sqlConfig)
      case "org.hsqldb.jdbc.JDBCDriver"                   => new SqlGeneratorHsqlDb(sqlConfig)
      case "com.ibm.db2.jcc.DB2Driver"                    => new SqlGeneratorDb2(sqlConfig)
      case d                                              =>
        log.warn(s"Unsupported JDBC driver: '$d'. Trying to use a generic SQL generator.")
        new SqlGeneratorGeneric(sqlConfig)
    }
    log.info(s"Using SQL generator: ${sqlGenerator.getClass.getCanonicalName}")
    sqlGenerator
  }
}
