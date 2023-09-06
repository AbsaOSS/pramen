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
import za.co.absa.pramen.core.app.config.{BookkeeperConfig, HadoopFormat, RuntimeConfig}
import za.co.absa.pramen.core.journal._
import za.co.absa.pramen.core.lock._
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
                (implicit spark: SparkSession): (Bookkeeper, TokenLockFactory, Journal, AutoCloseable) = {
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
            log.info(s"Using HadoopFS for lock management.")
            new TokenLockFactoryHadoop(spark.sparkContext.hadoopConfiguration, bookkeepingConfig.bookkeepingLocation.get + "/locks")
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
              log.info(s"Using Hadoop (CSV for records, JSON for schemas) for bookkeeping.")
              new BookkeeperText(bookkeepingConfig.bookkeepingLocation.get)
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
          log.info(s"Using HadoopFS to keep journal of executed jobs.")
          new JournalHadoop(bookkeepingConfig.bookkeepingLocation.get + "/journal")
      }
    }

    val closable = new AutoCloseable {
      override def close(): Unit = {
        mongoDbConnection.foreach(_.close())
        dbOpt.foreach(_.close())
      }
    }

    (bookkeeper, tokenFactory, journal, closable)
  }
}