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
import slick.jdbc.H2Profile
import slick.jdbc.H2Profile.api._
import za.co.absa.pramen.core.bookkeeper.model.{BookkeepingRecords, MetadataRecords, SchemaRecords}
import za.co.absa.pramen.core.journal.model.JournalTasks
import za.co.absa.pramen.core.lock.model.LockTickets
import za.co.absa.pramen.core.rdb.PramenDb.MODEL_VERSION
import za.co.absa.pramen.core.reader.JdbcUrlSelector
import za.co.absa.pramen.core.reader.model.JdbcConfig

import java.sql.Connection
import scala.util.control.NonFatal

class PramenDb(val jdbcConfig: JdbcConfig,
               val activeUrl: String,
               val jdbcConnection: Connection,
               val slickDb: Database) extends AutoCloseable {

  import za.co.absa.pramen.core.utils.FutureImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  val rdb: Rdb = new RdbJdbc(jdbcConnection)

  def db: Database = slickDb

  def setupDatabase(): Unit = {
    val dbVersion = rdb.getVersion()
    if (dbVersion < MODEL_VERSION) {
      initDatabase(dbVersion)
      rdb.setVersion(MODEL_VERSION)
    }
  }

  def initDatabase(dbVersion: Int): Unit = {
    log.warn(s"Initializing new database at $activeUrl")
    if (dbVersion < 1) {
      initTable(LockTickets.lockTickets.schema)
      initTable(JournalTasks.journalTasks.schema)
      initTable(BookkeepingRecords.records.schema)
    }
    if (dbVersion < 2) {
      initTable(SchemaRecords.records.schema)
    }
    if (dbVersion < 3) {
      initTable(MetadataRecords.records.schema)
    }

    if (0 < dbVersion && dbVersion < 4) {
      addColumn(JournalTasks.journalTasks.baseTableRow.tableName, "spark_application_id", "varchar(128)")
      addColumn(JournalTasks.journalTasks.baseTableRow.tableName, "pipelineId", "varchar(40)")
      addColumn(JournalTasks.journalTasks.baseTableRow.tableName, "pipelineName", "varchar(200)")
      addColumn(JournalTasks.journalTasks.baseTableRow.tableName, "environmentName", "varchar(128)")
      addColumn(JournalTasks.journalTasks.baseTableRow.tableName, "tenant", "varchar(200)")
    }
  }

  def initTable(schema: H2Profile.SchemaDescription): Unit = {
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

  def addColumn(table: String, columnName: String, columnType: String): Unit = {
    try {
      val quotedTable = s""""$table""""
      val quotedColumnName = s""""$columnName""""
      db.run(
          sqlu"ALTER TABLE #$quotedTable ADD #$quotedColumnName #$columnType"
        ).execute()
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(s"Unable to add column: '${columnName} ${columnType}' to table: '${table}'for the url: $activeUrl", ex)
    }
  }


  override def close(): Unit = {
    jdbcConnection.close()
    slickDb.close()
  }
}

object PramenDb {
  val MODEL_VERSION = 4
  val DEFAULT_RETRIES = 3

  def apply(jdbcConfig: JdbcConfig): PramenDb = {
    val (url, conn, database) = openDb(jdbcConfig)

    new PramenDb(jdbcConfig, url, conn, database)
  }

  private def openDb(jdbcConfig: JdbcConfig): (String, Connection, Database) = {
    val selector = JdbcUrlSelector(jdbcConfig)
    val (conn, url) = selector.getWorkingConnection(DEFAULT_RETRIES)
    val prop = selector.getProperties

    val database = jdbcConfig.user match {
      case Some(user) => Database.forURL(url = url, driver = jdbcConfig.driver, user = user, password = jdbcConfig.password.getOrElse(""), prop = prop, executor = AsyncExecutor("Rdb", 2, 10))
      case None       => Database.forURL(url = url, driver = jdbcConfig.driver, prop = prop, executor = AsyncExecutor("Rdb", 2, 10))
    }

    (url, conn, database)
  }

}

