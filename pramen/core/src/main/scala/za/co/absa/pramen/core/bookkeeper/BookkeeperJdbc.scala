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

import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile
import za.co.absa.pramen.core.bookkeeper.model._
import za.co.absa.pramen.core.model.{DataChunk, TableSchema}
import za.co.absa.pramen.core.rdb.PramenDb
import za.co.absa.pramen.core.rdb.PramenDb.DEFAULT_RETRIES
import za.co.absa.pramen.core.reader.JdbcUrlSelector
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.SlickUtils.WARN_IF_LONGER_MS
import za.co.absa.pramen.core.utils.{AlgorithmUtils, SlickUtils, TimeUtils}

import java.time.LocalDate
import scala.util.control.NonFatal

class BookkeeperJdbc(db: Database, profile: JdbcProfile, batchId: Long) extends BookkeeperBase(true, batchId) {
  import profile.api._
  import za.co.absa.pramen.core.utils.FutureImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)
  private val offsetManagement = new OffsetManagerCached(new OffsetManagerJdbc(db, batchId))
  @volatile private var isClosed = false

  override val bookkeepingEnabled: Boolean = true

  override def getLatestProcessedDateFromStorage(table: String, until: Option[LocalDate]): Option[LocalDate] = {
    val query = until match {
      case Some(endDate) =>
        val endDateStr = DataChunk.dateFormatter.format(endDate)
        BookkeepingRecords.records
          .filter(r => r.pramenTableName === table && r.infoDate <= endDateStr)
          .sortBy(r => (r.infoDate.desc, r.jobFinished.desc))
          .take(1)
      case None          =>
        BookkeepingRecords.records
          .filter(r => r.pramenTableName === table)
          .sortBy(r => (r.infoDate.desc, r.jobFinished.desc))
          .take(1)
    }

    val chunks = try {
      SlickUtils.executeQuery[BookkeepingRecords, BookkeepingRecord](db, query)
        .map(DataChunk.fromRecord)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the bookkeeping table.", ex)
    }

    if (chunks.isEmpty) {
      None
    } else {
      val chunk = chunks.maxBy(_.infoDateEnd)
      Option(LocalDate.parse(chunk.infoDateEnd, DataChunk.dateFormatter))
    }
  }

  override def getDataChunksFromStorage(table: String, infoDate: LocalDate, batchId: Option[Long]): Seq[DataChunk] = {
    val query = getFilter(table, Option(infoDate), Option(infoDate), batchId)

    try {
      SlickUtils.executeQuery[BookkeepingRecords, BookkeepingRecord](db, query)
        .map(DataChunk.fromRecord)
        .toArray[DataChunk]
        .sortBy(_.jobFinished)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the bookkeeping table.", ex)
    }
  }

  override def getLatestDataChunkFromStorage(table: String, infoDate: LocalDate): Option[DataChunk] = {
    val query = getFilter(table, Option(infoDate), Option(infoDate), None)
      .sortBy(r => r.jobFinished.desc)
      .take(1)

    try {
      val records = SlickUtils.executeQuery[BookkeepingRecords, BookkeepingRecord](db, query)
        .map(DataChunk.fromRecord)
        .toArray[DataChunk]

      if (records.length > 1)
        throw new IllegalStateException(s"More than one record was returned when only one was expected. Table: $table, infoDate: $infoDate")
      records.headOption
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the bookkeeping table.", ex)
    }
  }

  def getDataChunksCountFromStorage(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long = {
    val query = getFilter(table, dateBeginOpt, dateEndOpt, None)
      .length

    val count = try {
      SlickUtils.executeCount(db, query)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the bookkeeping table.", ex)
    }

    count
  }

  override def getDataAvailabilityFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataAvailability] = {
    val query = getFilter(table, Option(dateBegin), Option(dateEnd), None)
      .groupBy(r => r.infoDate)
      .map {case (infoDate, group) =>
        (infoDate, group.size, group.map(r => r.outputRecordCount).sum.getOrElse(0L))
      }
      .sortBy(_._1)

    try {
      SlickUtils.executeQuery[(Rep[String], Rep[Int], Rep[Long]), (String, Int, Long)](db, query)
        .map { case (infoDateStr, recordCount, outputRecordCount) =>
          val infoDate = LocalDate.parse(infoDateStr, DataChunk.dateFormatter)
          DataAvailability(infoDate, recordCount, outputRecordCount)
        }
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the bookkeeping table.", ex)
    }
  }

  override def saveRecordCountToStorage(table: String,
                                        infoDate: LocalDate,
                                        inputRecordCount: Long,
                                        outputRecordCount: Long,
                                        recordsAppended: Option[Long],
                                        jobStarted: Long,
                                        jobFinished: Long): Unit = {
    val dateStr = DataChunk.dateFormatter.format(infoDate)

    val record = BookkeepingRecord(table, dateStr, dateStr, dateStr, inputRecordCount, outputRecordCount, recordsAppended, jobStarted, jobFinished, Option(batchId))

    try {
      SlickUtils.ensureDbConnected(db)
      db.run(
        BookkeepingRecords.records += record
      ).execute()
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to write to the bookkeeping table.", ex)
    }
  }

  override def deleteNonCurrentBatchRecords(table: String, infoDate: LocalDate): Unit = {
    val dateStr = DataChunk.dateFormatter.format(infoDate)

    val query = BookkeepingRecords.records
      .filter(r => r.pramenTableName === table && r.infoDate === dateStr && r.batchId =!= Option(batchId))
      .delete

    try {
      AlgorithmUtils.runActionWithElapsedTimeEvent(WARN_IF_LONGER_MS) {
        db.run(query).execute()
      } { actualTimeMs =>
        val elapsedTime = TimeUtils.prettyPrintElapsedTimeShort(actualTimeMs)
        val sql = query.statements.mkString("; ")
        log.warn(s"Action execution time: $elapsedTime. SQL: $sql")
      }
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to delete non-current batch records from the bookkeeping table.", ex)
    }
  }

  override def deleteTable(tableName: String): Seq[String] = {
    val hasWildcard = tableName.contains("*")
    val tableNameEscaped = if (hasWildcard)
      tableName.trim.replace("%", "\\%").replace('*', '%')
    else
      tableName.trim.replace("%", "\\%")

    val likePattern = if (!hasWildcard)
      tableNameEscaped + "->%"
    else
      tableNameEscaped

    val patternForLogging = if (hasWildcard)
      s"'$likePattern'"
    else
      s"'$tableNameEscaped' or '$likePattern'"

    val listQuery = BookkeepingRecords.records
      .filter(r => r.pramenTableName === tableNameEscaped || r.pramenTableName.like(likePattern))
      .map(_.pramenTableName)
      .distinct

    val tablesToDelete = SlickUtils.executeQuery(db, listQuery).sorted

    if (tablesToDelete.length > 100)
      throw new IllegalArgumentException(s"The table wildcard '$tableName' matches more than 100 tables (${tablesToDelete.length}). To avoid accidental deletions, please refine the wildcard.")

    val deletionQuery = BookkeepingRecords.records
      .filter(r => r.pramenTableName === tableNameEscaped || r.pramenTableName.like(likePattern))
      .delete

    try {
      val deletedBkCount = SlickUtils.executeAction(db, deletionQuery)
      log.info(s"Deleted $deletedBkCount records from the bookkeeping table for tables matching $patternForLogging: ${tablesToDelete.mkString(", ")}")

      val deletedSchemaCount = SlickUtils.executeAction(db, SchemaRecords.records
        .filter(r => r.pramenTableName === tableNameEscaped || r.pramenTableName.like(likePattern))
        .delete
      )
      log.info(s"Deleted $deletedSchemaCount records from the schemas table.")

      val deletedOffsetsCount = SlickUtils.executeAction(db, OffsetRecords.records
        .filter(r => r.pramenTableName === tableNameEscaped || r.pramenTableName.like(likePattern))
        .delete
      )
      log.info(s"Deleted $deletedOffsetsCount records from the offsets table.")

      val deletedMetadataCount = SlickUtils.executeAction(db, MetadataRecords.records
        .filter(r => r.pramenTableName === tableNameEscaped || r.pramenTableName.like(likePattern))
        .delete
      )
      log.info(s"Deleted $deletedMetadataCount records from the metadata table.")

      tablesToDelete
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to delete records from the bookkeeping table for tables matching '$likePattern'.", ex)
    }
  }

  override def close(): Unit = {
    if (!isClosed) {
      db.close()
      isClosed = true
    }
  }

  private[pramen] override def getOffsetManager: OffsetManager = {
    offsetManagement
  }

  private def getFilter(tableName: String, infoDateBeginOpt: Option[LocalDate], infoDateEndOpt: Option[LocalDate], batchId: Option[Long]): Query[BookkeepingRecords, BookkeepingRecord, Seq] = {
    val baseFilter = (infoDateBeginOpt, infoDateEndOpt) match {
      case (Some(infoDateBegin), Some(infoDateEnd)) =>
        val date0Str = DataChunk.dateFormatter.format(infoDateBegin)
        val date1Str = DataChunk.dateFormatter.format(infoDateEnd)

        if (date0Str == date1Str) {
          BookkeepingRecords.records
            .filter(r => r.pramenTableName === tableName && r.infoDate === date0Str)
        } else {
          BookkeepingRecords.records
            .filter(r => r.pramenTableName === tableName && r.infoDate >= date0Str && r.infoDate <= date1Str)
        }
      case (Some(infoDateBegin), None) =>
        val date0Str = DataChunk.dateFormatter.format(infoDateBegin)

        BookkeepingRecords.records
          .filter(r => r.pramenTableName === tableName && r.infoDate >= date0Str)
      case (None, Some(infoDateEnd)) =>
        val date1Str = DataChunk.dateFormatter.format(infoDateEnd)

        BookkeepingRecords.records
          .filter(r => r.pramenTableName === tableName && r.infoDate <= date1Str)
      case (None, None) =>
        BookkeepingRecords.records
          .filter(r => r.pramenTableName === tableName)
    }

    batchId match {
      case Some(id) => baseFilter.filter(r => r.batchId === Option(id))
      case None => baseFilter
    }
  }

  override def getLatestSchema(table: String, infoDate: LocalDate): Option[(StructType, LocalDate)] = {
    val infoDateStr = infoDate.toString
    val query = SchemaRecords.records.filter(t => t.pramenTableName === table && t.infoDate <= infoDateStr)
      .sortBy(t => t.infoDate.desc)
      .take(1)

    SlickUtils.executeQuery[SchemaRecords, SchemaRecord](db, query)
      .map(schemaRecord => TableSchema(schemaRecord.pramenTableName, schemaRecord.infoDate, schemaRecord.schemaJson))
      .flatMap(tableSchema =>
        TableSchema.toSchemaAndDate(tableSchema)
      )
      .headOption
  }

  private[pramen] override def saveSchema(table: String, infoDate: LocalDate, schema: StructType): Unit = {
    val infoDateStr = infoDate.toString

    try {
      SlickUtils.ensureDbConnected(db)
      db.run(
        SchemaRecords.records.filter(t => t.pramenTableName === table && t.infoDate === infoDateStr).delete
      ).execute()

      db.run(
        SchemaRecords.records += SchemaRecord(table, infoDate.toString, schema.json)
      ).execute()
    } catch {
      case NonFatal(ex) => log.error(s"Unable to write to the bookkeeping schema table.", ex)
    }
  }

  /** This method is for migration purposes*/
  private[pramen] def saveSchemaRaw(table: String, infoDate: String, schema: String): Unit = {
    try {
      db.run(
        SchemaRecords.records.filter(t => t.pramenTableName === table && t.infoDate === infoDate).delete
      ).execute()

      db.run(
        SchemaRecords.records += SchemaRecord(table, infoDate, schema)
      ).execute()
    } catch {
      case NonFatal(ex) => log.error(s"Unable to write to the bookkeeping schema table.", ex)
    }
  }
}

object BookkeeperJdbc {
  def fromJdbcConfig(jdbcConfig: JdbcConfig, batchId: Long): BookkeeperJdbc = {
    val selector = JdbcUrlSelector(jdbcConfig)
    val url = selector.getWorkingUrl(DEFAULT_RETRIES)
    val prop = selector.getProperties

    val profile = PramenDb.getProfile(jdbcConfig.driver)

    val db = if (jdbcConfig.user.nonEmpty) {
      Database.forURL(url = url, driver = jdbcConfig.driver, user = jdbcConfig.user.get, password = jdbcConfig.password.getOrElse(""), prop = prop)
    } else {
      Database.forURL(url = url, driver = jdbcConfig.driver, prop = prop)
    }
    new BookkeeperJdbc(db,  profile, batchId)
  }

}
