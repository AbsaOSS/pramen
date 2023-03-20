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
import za.co.absa.pramen.core.utils.hive.QueryExecutorJdbc.DEFAULT_NUMBER_OF_RETRIES

import java.sql.{Connection, ResultSet, SQLSyntaxErrorException}
import scala.util.Try
import scala.util.control.NonFatal

class QueryExecutorJdbc(jdbcUrlSelector: JdbcUrlSelector) extends QueryExecutor {
  private val log = LoggerFactory.getLogger(this.getClass)
  private var connection: Connection = _
  private val retries = Math.max(jdbcUrlSelector.getNumberOfUrls + 1, DEFAULT_NUMBER_OF_RETRIES)

  override def doesTableExist(dbName: String, tableName: String): Boolean = {
    val query = s"SELECT 1 FROM $tableName WHERE 0 = 1"

    Try {
      execute(query)
    }.isSuccess
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

  private[core] def executeActionOnConnection(action: Connection => Unit): Unit = {
    try {
      action(getConnection(forceReconnect = false))
    } catch {
      case NonFatal(ex) =>
        log.warn(s"Got an error on existing connection. Retrying...", ex)
        action(getConnection(forceReconnect = true))
    }
  }

  def getConnection(forceReconnect: Boolean): Connection = {
    if (connection == null || forceReconnect) {
      val (newConnection, url) = jdbcUrlSelector.getWorkingConnection(retries)
      log.info(s"Selected query executor connection: $url")
      connection = newConnection
    }
    connection
  }
}

object QueryExecutorJdbc {
  private val DEFAULT_NUMBER_OF_RETRIES = 3

  def fromJdbcConfig(jdbcConfig: JdbcConfig): QueryExecutorJdbc = {
    val jdbcUrlSelector = JdbcUrlSelector(jdbcConfig)

    new QueryExecutorJdbc(jdbcUrlSelector)
  }
}
