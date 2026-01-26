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
import za.co.absa.pramen.core.rdb.PramenDb.DEFAULT_RETRIES
import za.co.absa.pramen.core.rdb.RdbJdbc.dbVersionTableName
import za.co.absa.pramen.core.reader.JdbcUrlSelector
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.UsingUtils

import java.sql.{Connection, SQLException}
import scala.util.control.NonFatal

class RdbJdbc(val connection: Connection) extends AutoCloseable with Rdb{
  private val log = LoggerFactory.getLogger(this.getClass)

  override def getVersion(): Int = {
    getDbVersion()
  }

  override def setVersion(version: Int): Unit = {
    if (!doesTableExists(dbVersionTableName)) {
      executeDDL(s"CREATE TABLE IF NOT EXISTS $dbVersionTableName (version INTEGER NOT NULL, PRIMARY KEY (version));")
      executeDDL(s"INSERT INTO $dbVersionTableName (version) VALUES (0)")
    }

    executeDDL(s"UPDATE $dbVersionTableName SET version = $version")
  }

  override def doesTableExists(tableName: String): Boolean = {
    val meta = connection.getMetaData
    try {
      val res = meta.getTables(null, null, tableName, Array[String]("TABLE"))
      if (res.next) {
        res.close()
        true
      } else {
        res.close()
        false
      }
    } catch {
      case ex: SQLException =>
        log.warn(s"Error while checking existence of $tableName.", ex)
        false
    }
  }

  override def executeDDL(ddl: String): Unit = {
    UsingUtils.using(connection.createStatement()) { statement =>
      statement.execute(ddl)
    }
  }

  private def getDbVersion(): Int = {
    val statement = connection.createStatement()
    val dbVersion = try {
      val rs = statement.executeQuery(s"SELECT version FROM $dbVersionTableName;")
      rs.next()
      rs.getInt(1)
    } catch {
      case NonFatal(_) => 0
    }

    statement.close()
    dbVersion
  }

  override def close(): Unit = if (!connection.isClosed) connection.close()
}

object RdbJdbc {
  val dbVersionTableName = "db_version"

  def apply(jdbcConfig: JdbcConfig): RdbJdbc = {
    val numberOfAttempts = jdbcConfig.retries.getOrElse(DEFAULT_RETRIES)
    val selector = JdbcUrlSelector(jdbcConfig)
    val (conn, _) = selector.getWorkingConnection(numberOfAttempts)

    new RdbJdbc(conn)
  }
}
