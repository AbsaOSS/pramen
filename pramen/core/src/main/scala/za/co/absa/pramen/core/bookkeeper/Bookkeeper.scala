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

package za.co.absa.pramen.core.bookkeeper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.MetadataManager
import za.co.absa.pramen.core.app.config.{BookkeeperConfig, HadoopFormat, RuntimeConfig}
import za.co.absa.pramen.core.journal._
import za.co.absa.pramen.core.lock._
import za.co.absa.pramen.core.metadata.{MetadataManagerJdbc, MetadataManagerNull}
import za.co.absa.pramen.core.model.DataChunk
import za.co.absa.pramen.core.mongo.MongoDbConnection
import za.co.absa.pramen.core.rdb.PramenDb

import java.time.LocalDate

/**
  * A bookkeeper is responsible of querying and updating state of all tables related to an ingestion pipeline.
  */
trait Bookkeeper {
  val bookkeepingEnabled: Boolean

  def getLatestProcessedDate(table: String, until: Option[LocalDate] = None): Option[LocalDate]

  def getLatestDataChunk(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Option[DataChunk]

  def getDataChunks(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataChunk]

  def getDataChunksCount(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long

  private[pramen] def setRecordCount(table: String,
                                     infoDate: LocalDate,
                                     infoDateBegin: LocalDate,
                                     infoDateEnd: LocalDate,
                                     inputRecordCount: Long,
                                     outputRecordCount: Long,
                                     jobStarted: Long,
                                     jobFinished: Long,
                                     isTableTransient: Boolean): Unit

  def getLatestSchema(table: String, until: LocalDate): Option[(StructType, LocalDate)]

  private[pramen] def saveSchema(table: String, infoDate: LocalDate, schema: StructType): Unit
}

object Bookkeeper {
  private val log = LoggerFactory.getLogger(this.getClass)

  def fromConfig(bookkeepingConfig: BookkeeperConfig, runtimeConfig: RuntimeConfig)
                (implicit spark: SparkSession): (Bookkeeper, TokenLockFactory, Journal, MetadataManager, AutoCloseable) = {
    val mongoDbConnection = bookkeepingConfig.bookkeepingConnectionString.map { url =>
      MongoDbConnection.getConnection(url, bookkeepingConfig.bookkeepingDbName.get)
    }

    val hasBookkeepingJdbc = bookkeepingConfig.bookkeepingJdbcConfig.exists(_.primaryUrl.isDefined)

    val dbOpt = if (hasBookkeepingJdbc) {
      val jdbcConfig = bookkeepingConfig.bookkeepingJdbcConfig.get
      val syncDb = PramenDb(jdbcConfig)
      syncDb.setupDatabase()
      Option(syncDb)
    } else None

    val tokenFactory = if (runtimeConfig.useLocks && bookkeepingConfig.bookkeepingEnabled) {
      if (hasBookkeepingJdbc) {
        log.info(s"Using RDB for lock management.")
        new TokenLockFactoryJdbc(dbOpt.get.slickDb)
      } else {
        mongoDbConnection match {
          case Some(connection) =>
            log.info(s"Using MongoDB for lock management.")
            new TokenLockFactoryMongoDb(connection)
          case None =>
            bookkeepingConfig.bookkeepingLocation match {
              case Some(path) =>
                log.info(s"Using HadoopFS for lock management at $path/locks")
                new TokenLockFactoryHadoopPath(spark.sparkContext.hadoopConfiguration, path + "/locks")
              case None =>
                log.warn(s"Locking is DISABLED.")
                new TokenLockFactoryAllow
            }
        }
      }
    } else {
      log.warn(s"Locking is DISABLED.")
      new TokenLockFactoryAllow
    }

    val bookkeeper = if (!bookkeepingConfig.bookkeepingEnabled) {
      log.info(s"Bookkeeping is DISABLED. Updates won't be tracked")
      new BookkeeperNull()
    } else if (hasBookkeepingJdbc) {
      new BookkeeperJdbc(dbOpt.get.slickDb)
    } else {
      mongoDbConnection match {
        case Some(connection) =>
          log.info(s"Using MongoDB for bookkeeping.")
          new BookkeeperMongoDb(connection)
        case None =>
          bookkeepingConfig.bookkeepingHadoopFormat match {
            case HadoopFormat.Text =>
              val path = bookkeepingConfig.bookkeepingLocation.get
              log.info(s"Using Hadoop (CSV for records, JSON for schemas) for bookkeeping at $path")
              new BookkeeperText(path)
            case HadoopFormat.Delta =>
              bookkeepingConfig.deltaTablePrefix match {
                case Some(tablePrefix) =>
                  val fullTableName = BookkeeperDeltaTable.getFullTableName(bookkeepingConfig.deltaDatabase, tablePrefix, "*")
                  log.info(s"Using Delta Lake managed table '$fullTableName' for bookkeeping.")
                  new BookkeeperDeltaTable(bookkeepingConfig.deltaDatabase, tablePrefix)
                case None =>
                  val path = bookkeepingConfig.bookkeepingLocation.get
                  log.info(s"Using Delta Lake for bookkeeping at $path")
                  new BookkeeperDeltaPath(path)
              }
          }
      }
    }

    val journal: Journal = if (!bookkeepingConfig.bookkeepingEnabled) {
      log.info(s"The journal is DISABLED.")
      new JournalNull()
    } else if (hasBookkeepingJdbc) {
      log.info(s"Using RDB to keep journal of executed jobs.")
      new JournalJdbc(dbOpt.get.slickDb)
    } else {
      mongoDbConnection match {
        case Some(connection) =>
          log.info(s"Using MongoDB to keep journal of executed jobs.")
          new JournalMongoDb(connection)
        case None =>
          bookkeepingConfig.bookkeepingHadoopFormat match {
            case HadoopFormat.Text =>
              val path = bookkeepingConfig.bookkeepingLocation.get + "/journal"
              log.info(s"Using HadoopFS to keep journal of executed jobs at $path")
              new JournalHadoopCsv(path)
            case HadoopFormat.Delta =>
              bookkeepingConfig.deltaTablePrefix match {
                case Some(tablePrefix) =>
                  val fullTableName = JournalHadoopDeltaTable.getFullTableName(bookkeepingConfig.deltaDatabase, tablePrefix)
                  log.info(s"Using Delta Lake managed table '$fullTableName' for the journal.")
                  new JournalHadoopDeltaTable(bookkeepingConfig.deltaDatabase, tablePrefix)
                case None =>
                  val path = bookkeepingConfig.bookkeepingLocation.get + "/journal"
                  log.info(s"Using Delta Lake for the journal at $path")
                  new JournalHadoopDeltaPath(path)
              }
          }

      }
    }

    val metadataManager = if (!bookkeepingConfig.bookkeepingEnabled) {
      log.info(s"The custom metadata persistence is DISABLED.")
      new MetadataManagerNull(isPersistenceEnabled = false)
    } else if (hasBookkeepingJdbc) {
      log.info(s"Using RDB to keep custom metadata.")
      new MetadataManagerJdbc(dbOpt.get.slickDb)
    } else {
      log.info(s"The custom metadata management is not supported.")
      new MetadataManagerNull(isPersistenceEnabled = true)
    }

    val closable = new AutoCloseable {
      override def close(): Unit = {
        mongoDbConnection.foreach(_.close())
        dbOpt.foreach(_.close())
      }
    }

    (bookkeeper, tokenFactory, journal, metadataManager, closable)
  }
}
