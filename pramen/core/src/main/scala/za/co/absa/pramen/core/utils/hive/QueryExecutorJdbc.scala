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

package za.co.absa.pramen.core.utils.hive

import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.reader.JdbcUrlSelector
import za.co.absa.pramen.core.reader.model.JdbcConfig

import java.sql._
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

class QueryExecutorJdbc(jdbcUrlSelector: JdbcUrlSelector, optimizedExistQuery: Boolean) extends QueryExecutor {
  import QueryExecutorJdbc._

  private val log = LoggerFactory.getLogger(this.getClass)
  private var connection: Connection = _
  private val defaultRetries = jdbcUrlSelector.getNumberOfUrls
  private val retries =
    jdbcUrlSelector.jdbcConfig.retries.getOrElse(defaultRetries)

  override def doesTableExist(dbName: Option[String], tableName: String): Boolean = {
    val fullTableName = HiveHelper.getFullTable(dbName, tableName)

    if (optimizedExistQuery) {
      val exists = checkTableExistenceUsingJdbcNative(dbName, tableName)
      if (exists)
         log.info(s"Table $fullTableName exists.")
      else {
        log.info(s"Table $fullTableName does not exist.")
      }
      exists
    } else {
      val query = s"SELECT 1 FROM $fullTableName WHERE 0 = 1"
      Try {
        execute(query)
      } match {
        case Failure(ex) =>
          log.info(s"The query resulted in an error, assuming the table $fullTableName does not exist (${ex.getMessage}).")
          false
        case _ =>
          log.info(s"Table $fullTableName exists.")
          true
      }
    }
  }

  @throws[SQLSyntaxErrorException]
  override def execute(query: String): Unit = {
    log.info(s"Executing SQL: $query")

    executeActionOnConnection { conn =>
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      try {
        statement.execute(query)
      } finally {
        statement.close()
      }
    }
  }

  override def close(): Unit = if (connection != null) connection.close()

  private[core] def executeActionOnConnection(action: Connection => Boolean): Boolean = {
    val currentConnection = getConnection(forceReconnect = false)
    try {
      action(currentConnection)
    } catch {
      case ex: SQLException =>
        throw ex
      case NonFatal(ex) =>
        log.warn(s"Got an error on existing connection. Retrying...", ex)
        action(getConnection(forceReconnect = true))
    }
  }

  def getConnection(forceReconnect: Boolean): Connection = {
    if (connection == null) {
      jdbcUrlSelector.logConnectionSettings()
    }

    if (connection == null || forceReconnect) {
      val (newConnection, url) = jdbcUrlSelector.getWorkingConnection(retries)
      log.info(s"Selected query executor connection: $url")
      connection = newConnection
    }
    connection
  }

  def checkTableExistenceUsingJdbcNative(databaseNameOpt: Option[String], tableName: String): Boolean = {
    import za.co.absa.pramen.core.utils.UsingUtils.Implicits._

    val conn = getConnection(false)
    val metadata = conn.getMetaData

    val db = databaseNameOpt match {
      case Some(s) => getEscapedMetadataString(s, metadata)
      case None => null
    }

    val table = getEscapedMetadataString(tableName, metadata)

    for (rs <- metadata.getTables(null, db, table, HIVE_TABLE_TYPES)) yield {
      val exists = rs.next
      exists
    }
  }

  def getEscapedMetadataString(s: String, metaData: DatabaseMetaData): String = {
    val esc = metaData.getSearchStringEscape

    s.replace(esc, s"${esc}$esc")
      .replace("%", s"$esc%")
      .replace("_", s"${esc}_")
  }
}

object QueryExecutorJdbc {
  val HIVE_TABLE_TYPES = scala.Array[String]("TABLE", "VIEW", "MATERIALIZED VIEW")

  def fromJdbcConfig(jdbcConfig: JdbcConfig, optimizedExistQuery: Boolean): QueryExecutorJdbc = {
    val jdbcUrlSelector = JdbcUrlSelector(jdbcConfig)

    new QueryExecutorJdbc(jdbcUrlSelector, optimizedExistQuery)
  }
}
