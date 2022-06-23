/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.rdb

import org.slf4j.LoggerFactory
import slick.jdbc.H2Profile
import slick.jdbc.H2Profile.api._
import za.co.absa.pramen.framework.bookkeeper.model.{BookkeepingRecords, SchemaRecords}
import za.co.absa.pramen.framework.journal.model.JournalTasks
import za.co.absa.pramen.framework.lock.model.LockTickets
import za.co.absa.pramen.framework.rdb.PramenDb.MODEL_VERSION
import za.co.absa.pramen.framework.reader.model.JdbcConfig
import za.co.absa.pramen.framework.utils.JdbcNativeUtils

import java.sql.Connection
import scala.util.control.NonFatal

class PramenDb(val jdbcConfig: JdbcConfig,
               val activeUrl: String,
               val jdbcConnection: Connection,
               val slickDb: Database) extends AutoCloseable {

  import za.co.absa.pramen.framework.utils.FutureImplicits._

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

  override def close(): Unit = {
    jdbcConnection.close()
    slickDb.close()
  }
}

object PramenDb {
  val MODEL_VERSION = 2

  def apply(jdbcConfig: JdbcConfig): PramenDb = {
    val (url, conn, database) = openDb(jdbcConfig)

    new PramenDb(jdbcConfig, url, conn, database)
  }

  private def openDb(jdbcConfig: JdbcConfig): (String, Connection, Database) = {
    val (url, conn) = JdbcNativeUtils.getConnection(jdbcConfig)

    val database = Database.forURL(url = url,
      user = jdbcConfig.user,
      password = jdbcConfig.password,
      driver = jdbcConfig.driver,
      executor = AsyncExecutor("Rdb", 2, 10))

    (url, conn, database)
  }

}

