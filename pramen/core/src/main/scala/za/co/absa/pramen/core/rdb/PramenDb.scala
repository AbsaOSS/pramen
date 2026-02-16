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

package za.co.absa.pramen.core.rdb

import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.{JdbcBackend, JdbcProfile}
import slick.util.AsyncExecutor
import za.co.absa.pramen.api.Pramen
import za.co.absa.pramen.core.bookkeeper.model.{BookkeepingTable, MetadataTable, OffsetTable, SchemaTable}
import za.co.absa.pramen.core.journal.model.JournalTable
import za.co.absa.pramen.core.lock.model.LockTicketTable
import za.co.absa.pramen.core.rdb.PramenDb.MODEL_VERSION
import za.co.absa.pramen.core.reader.JdbcUrlSelector
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.{AlgorithmUtils, UsingUtils}

import java.sql.Connection
import scala.util.Try
import scala.util.control.NonFatal

class PramenDb(val jdbcConfig: JdbcConfig,
               val activeUrl: String,
               val slickDb: Database,
               val slickProfile: JdbcProfile) extends AutoCloseable {
  def db: Database = slickDb

  import slickProfile.api._
  import za.co.absa.pramen.core.utils.FutureImplicits._

  val bookkeepingTable: BookkeepingTable = new BookkeepingTable {
    override val profile = slickProfile
  }
  private val schemaTable = new SchemaTable {
    override val profile = slickProfile
  }
  val offsetTable: OffsetTable = new OffsetTable {
    override val profile = slickProfile
  }
  val journalTable: JournalTable = new JournalTable {
    override val profile = slickProfile
  }
  val lockTicketTable: LockTicketTable = new LockTicketTable {
    override val profile = slickProfile
  }
  val metadataTable: MetadataTable = new MetadataTable {
    override val profile = slickProfile
  }

  private val log = LoggerFactory.getLogger(this.getClass)

  private def setupDatabase(jdbcConnection: Connection): Unit = {
    // Explicitly set auto-commit to true, overriding any user JDBC settings or PostgreSQL defaults
    Try(jdbcConnection.setAutoCommit(true)).recover {
      case NonFatal(e) => log.warn(s"Unable to set autoCommit=true for the bookkeeping database that uses the driver: ${jdbcConfig.driver}.")
    }

    UsingUtils.using(new RdbJdbc(jdbcConnection)) { rdb =>
      val dbVersion = rdb.getVersion()
      if (dbVersion < MODEL_VERSION) {
        initDatabase(dbVersion)
        rdb.setVersion(MODEL_VERSION)
      }
    }
  }

  private def initDatabase(dbVersion: Int): Unit = {
    log.warn(s"Initializing new database at $activeUrl")
    if (dbVersion < 1) {
      initTable(lockTicketTable.records.schema)
      initTable(journalTable.records.schema)
      initTable(bookkeepingTable.records.schema)
    }
    if (dbVersion < 2) {
      initTable(schemaTable.records.schema)
    }
    if (dbVersion < 3) {
      initTable(metadataTable.records.schema)
    }

    if (0 < dbVersion && dbVersion < 4) {
      addColumn(journalTable.records.baseTableRow.tableName, "spark_application_id", "varchar(128)")
      addColumn(journalTable.records.baseTableRow.tableName, "pipelineId", "varchar(40)")
      addColumn(journalTable.records.baseTableRow.tableName, "pipelineName", "varchar(200)")
      addColumn(journalTable.records.baseTableRow.tableName, "environmentName", "varchar(128)")
      addColumn(journalTable.records.baseTableRow.tableName, "tenant", "varchar(200)")
    }

    if (dbVersion < 5) {
      initTable(offsetTable.records.schema)
    }

    if (0 < dbVersion && dbVersion < 6) {
      addColumn(journalTable.records.baseTableRow.tableName, "appended_record_count", "bigint")
    }

    if (0 < dbVersion && dbVersion < 7) {
      addColumn(lockTicketTable.records.baseTableRow.tableName, "created_at", "bigint")
    }

    if (0 < dbVersion && dbVersion < 8) {
      addColumn(journalTable.records.baseTableRow.tableName, "country", "varchar(50)")
    }

    if (0 < dbVersion && dbVersion < 9) {
      addColumn(bookkeepingTable.records.baseTableRow.tableName, "batch_id", "bigint")
      addColumn(bookkeepingTable.records.baseTableRow.tableName, "appended_record_count", "bigint")
      addColumn(journalTable.records.baseTableRow.tableName, "batch_id", "bigint")
    }
  }

  private def initTable(schema: slickProfile.SchemaDescription): Unit = {
    try {
      db.run(DBIO.seq(
        schema.createIfNotExists
      )).execute()
    } catch {
      case NonFatal(ex) =>
        val sql = schema.createIfNotExists.statements.mkString(";")
        throw new RuntimeException(s"Unable to initialize the table for the url: $activeUrl. SQL: $sql", ex)
    }
  }

  private def addColumn(table: String, columnName: String, columnType: String): Unit = {
    try {
      val quotedTable = s""""$table""""
      val quotedColumnName = s""""$columnName""""
      db.run(
          sqlu"ALTER TABLE #$quotedTable ADD #$quotedColumnName #$columnType"
        ).execute()
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to add column: '$columnName $columnType' to table: '$table 'for the url: $activeUrl", ex)
    }
  }


  override def close(): Unit = {
    try {
      slickDb.close()
    } catch {
      case NonFatal(ex) =>
        log.warn("Error closing the Pramen RDB database connection.", ex)
    }

  }
}

object PramenDb {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val conf = Pramen.getConfig

  val MODEL_VERSION = 9
  val DEFAULT_RETRIES: Int = conf.getInt("pramen.internal.connection.retries.default")
  val BACKOFF_MIN_MS: Int = conf.getInt("pramen.internal.connection.backoff.min.ms")
  val BACKOFF_MAX_MS: Int = conf.getInt("pramen.internal.connection.backoff.max.ms")

  def apply(jdbcConfig: JdbcConfig): PramenDb = {
    val (url, connection) = getConnection(jdbcConfig)

    UsingUtils.using(connection) { conn =>
      val (database, profile) = openDb(jdbcConfig, url)
      val pramenDb = new PramenDb(jdbcConfig, url, database, profile)
      pramenDb.setupDatabase(conn)
      pramenDb
    }
  }

  def getProfile(driver: String): JdbcProfile = {
    driver match {
      case "org.postgresql.Driver"      => slick.jdbc.PostgresProfile
      case "org.hsqldb.jdbc.JDBCDriver" => slick.jdbc.HsqldbProfile
      case "org.h2.Driver"              => slick.jdbc.H2Profile
      case "org.sqlite.JDBC"            => slick.jdbc.SQLiteProfile
      case "com.mysql.cj.jdbc.Driver" | "com.mysql.jdbc.Driver" =>
        slick.jdbc.MySQLProfile
      case "com.microsoft.sqlserver.jdbc.SQLServerDriver" | "net.sourceforge.jtds.jdbc.Driver" =>
        slick.jdbc.SQLServerProfile
      case other => throw new IllegalArgumentException(s"Unknown driver for the bookkeeping database: $other")
    }
  }

  def getConnection(jdbcConfig: JdbcConfig): (String, Connection) = {
    val numberOfAttempts = jdbcConfig.retries.getOrElse(DEFAULT_RETRIES)
    val selector = JdbcUrlSelector(jdbcConfig)
    val (conn, url) = selector.getWorkingConnection(numberOfAttempts)

    (url, conn)
  }

  def openDb(jdbcConfig: JdbcConfig, workingUrl: String): (Database, JdbcProfile) = {
    val numberOfAttempts = jdbcConfig.retries.getOrElse(DEFAULT_RETRIES)
    val selector = JdbcUrlSelector(jdbcConfig)
    val prop = selector.getProperties

    val slickProfile = getProfile(jdbcConfig.driver)

    var database: JdbcBackend.DatabaseDef = null
    AlgorithmUtils.actionWithRetry(numberOfAttempts, log, BACKOFF_MIN_MS, BACKOFF_MAX_MS) {
      database = jdbcConfig.user match {
        case Some(user) => Database.forURL(url = workingUrl, driver = jdbcConfig.driver, user = user, password = jdbcConfig.password.getOrElse(""), prop = prop, executor = AsyncExecutor("Rdb", 2, 10))
        case None       => Database.forURL(url = workingUrl, driver = jdbcConfig.driver, prop = prop, executor = AsyncExecutor("Rdb", 2, 10))
      }
    }

    (database, slickProfile)
  }
}

