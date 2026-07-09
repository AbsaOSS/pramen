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
import za.co.absa.pramen.core.runner.task.ThreadClosableRegistry

import java.sql._
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

class QueryExecutorJdbc(jdbcUrlSelector: JdbcUrlSelector, existenceCheckStrategy: ExistenceCheckStrategy) extends QueryExecutor {
  import QueryExecutorJdbc._

  private val log = LoggerFactory.getLogger(this.getClass)
  private var connection: Connection = _
  private val defaultRetries = jdbcUrlSelector.getNumberOfUrls
  private val retries =
    jdbcUrlSelector.jdbcConfig.retries.getOrElse(defaultRetries)

  override def doesTableExist(dbName: Option[String], tableName: String): Boolean = {
    val fullTableName = HiveHelper.getFullTable(dbName, tableName)

    val exists = existenceCheckStrategy match {
      case ExistenceCheckStrategy.MetadataQuery =>
        doesTableExistUsingHiveMetadata(dbName, tableName)
      case ExistenceCheckStrategy.DescribeTable =>
        doesTableExistUsingDescribeTable(dbName, tableName)
      case ExistenceCheckStrategy.MetadataAndDescribeQuery =>
        if (doesTableExistUsingHiveMetadata(dbName, tableName)) {
          true
        } else {
          doesTableExistUsingDescribeTable(dbName, tableName)
        }
      case ExistenceCheckStrategy.SelectQuery =>
        doesTableExistUsingSqlQuery(dbName, tableName)
    }

    if (exists)
      log.info(s"Table $fullTableName exists.")
    else {
      log.info(s"Table $fullTableName does not exist.")
    }
    exists
  }

  @throws[SQLSyntaxErrorException]
  override def execute(query: String): Unit = {
    log.info(s"Executing SQL: $query")

    executeActionOnConnection { conn =>
      if (Thread.currentThread().isInterrupted) {
        throw new InterruptedException(s"Current thread is interrupted. Aborting SQL execution: $query")
      }
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val autoCloseStatement: AutoCloseable = new AutoCloseable {
        val statementClosed = new java.util.concurrent.atomic.AtomicBoolean(false)

        override def close(): Unit = {
          if (statementClosed.compareAndSet(false, true)) {
            try {
              log.debug(s"Cancelling SQL statement: $query")
              statement.cancel()
            } finally {
              log.debug(s"Closing the SQL statement...")
              statement.close()
            }
          }
        }
      }

      ThreadClosableRegistry.registerCloseable(autoCloseStatement)

      try {
        statement.execute(query)
      } finally {
        ThreadClosableRegistry.unregisterCloseable(autoCloseStatement)
        autoCloseStatement.close()
      }
    }
  }

  override def close(): Unit = {
    if (connection != null) {
      ThreadClosableRegistry.unregisterCloseable(connection)
      try {
        connection.close()
      } catch {
        case ex: InterruptedException => throw ex
        case NonFatal(ex) => log.warn("Failed to close JDBC connection", ex)
      }
    }
  }

  private[core] def executeActionOnConnection(action: Connection => Boolean): Boolean = {
    val currentConnection = getConnection(forceReconnect = false)
    try {
      action(currentConnection)
    } catch {
      case ex: SQLException =>
        throw ex
      case ex: InterruptedException =>
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
      val (newConnection, url) = jdbcUrlSelector.getNewConnection(retries)
      log.info(s"Selected query executor connection: $url")
      close()
      connection = newConnection
      ThreadClosableRegistry.registerCloseable(connection)
    }
    connection
  }

  def doesTableExistUsingHiveMetadata(databaseNameOpt: Option[String], tableName: String): Boolean = {
    import za.co.absa.pramen.core.utils.UsingUtils.Implicits._

    try {
      val conn = getConnection(false)
      val metadata = conn.getMetaData

      val db = databaseNameOpt match {
        case Some(s) => getEscapedMetadataString(s, metadata)
        case None => null
      }

      val table = getEscapedMetadataString(tableName, metadata)

      log.info(s"Checking existence of table '$db.$tableName' using metadata query...")
      val exists = for (rs <- metadata.getTables(null, db, table, HIVE_TABLE_TYPES)) yield {
        val r = rs.next
        r
      }
      log.info(s"Table '$db.$tableName' exists = $exists")
      exists
    } catch {
      case ex: InterruptedException =>
        throw ex
      case NonFatal(ex) =>
        log.warn(s"Metadata table existence check failed for '$tableName'. Falling back to DESCRIBE TABLE (${ex.getMessage}).")
        false
    }
  }

  def doesTableExistUsingDescribeTable(databaseNameOpt: Option[String], tableName: String): Boolean = {
    val fullTableName = HiveHelper.getFullTable(databaseNameOpt, tableName)

    val query = s"DESCRIBE $fullTableName"

    val exists = Try {
      log.info(s"Checking existence of table '$fullTableName' using DESCRIBE TABLE query...")
      execute(query)
    } match {
      case Failure(ex) =>
        ex match {
          case ex: InterruptedException =>
            throw ex
          case ex: Throwable =>
            log.info(s"The query resulted in an error, assuming the table $fullTableName does not exist (${ex.getMessage}).")
        }
        false
      case _ =>
        true
    }
    log.info(s"Table '$fullTableName' exists = $exists")
    exists
  }

  def doesTableExistUsingSqlQuery(databaseNameOpt: Option[String], tableName: String): Boolean = {
    val fullTableName = HiveHelper.getFullTable(databaseNameOpt, tableName)

    val query = s"SELECT 1 FROM $fullTableName WHERE 0 = 1"
    val exists = Try {
      log.info(s"Checking existence of table '$fullTableName' using the query: '$query'...")
      execute(query)
    } match {
      case Failure(ex) =>
        ex match {
          case ex: InterruptedException =>
            throw ex
          case ex: Throwable =>
            log.info(s"The query resulted in an error, assuming the table $fullTableName does not exist (${ex.getMessage}).")
        }
        false
      case _ =>
        true
    }
    log.info(s"Table '$fullTableName' exists = $exists")
    exists
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

  def fromJdbcConfig(jdbcConfig: JdbcConfig, existenceCheckStrategy: ExistenceCheckStrategy): QueryExecutorJdbc = {
    val jdbcUrlSelector = JdbcUrlSelector(jdbcConfig)

    new QueryExecutorJdbc(jdbcUrlSelector, existenceCheckStrategy)
  }
}
