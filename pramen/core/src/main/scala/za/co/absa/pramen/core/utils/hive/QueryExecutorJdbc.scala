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

import java.sql.{Connection, ResultSet, SQLException, SQLSyntaxErrorException}
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

class QueryExecutorJdbc(jdbcUrlSelector: JdbcUrlSelector) extends QueryExecutor {
  private val log = LoggerFactory.getLogger(this.getClass)
  private var connection: Connection = _
  private val defaultRetries = jdbcUrlSelector.getNumberOfUrls
  private val retries =
    jdbcUrlSelector.jdbcConfig.retries.getOrElse(defaultRetries)

  override def doesTableExist(dbName: Option[String], tableName: String): Boolean = {
    if (jdbcUrlSelector.jdbcConfig.optimizedExistQuery) {
      val query = dbName match {
        case Some(db) => s"SHOW TABLES IN $db LIKE '${unEscape(tableName)}'"
        case None => s"SHOW TABLES LIKE '${unEscape(tableName)}'"
      }

      Try {
        executeNonEmpty(query)
      } match {
        case scala.util.Success(res) => res
        case Failure(ex: Throwable) =>
          log.warn(s"Got an error while checking if table exists", ex)
          false
      }
    } else {
      val fullTableName = HiveHelper.getFullTable(dbName, tableName)

      val query = s"SELECT 1 FROM $fullTableName WHERE 0 = 1"

      Try {
        execute(query)
      }.isSuccess
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

  def executeNonEmpty(query: String): Boolean = {
    log.info(s"Executing SQL: $query")

    executeActionOnConnection { conn =>
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery(query)

      try {
        // If the result set is non-empty this will return true, otherwise it will return false.
        rs.next()
      } finally {
        rs.close()
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

  private def unEscape(s: String): String = {
    if (s.length > 1) {
      if (s.head == '`' && s.last == '`') {
        s.substring(1, s.length - 1)
      } else {
        s
      }
    } else {
      s
    }
  }
}

object QueryExecutorJdbc {
  def fromJdbcConfig(jdbcConfig: JdbcConfig): QueryExecutorJdbc = {
    val jdbcUrlSelector = JdbcUrlSelector(jdbcConfig)

    new QueryExecutorJdbc(jdbcUrlSelector)
  }
}
