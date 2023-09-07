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

import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import slick.jdbc.H2Profile.api._
import za.co.absa.pramen.core.bookkeeper.model.{BookkeepingRecord, BookkeepingRecords, SchemaRecord, SchemaRecords}
import za.co.absa.pramen.core.model.{DataChunk, TableSchema}
import za.co.absa.pramen.core.rdb.PramenDb.DEFAULT_RETRIES
import za.co.absa.pramen.core.reader.JdbcUrlSelector
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.SlickUtils

import java.time.LocalDate
import scala.util.control.NonFatal

class BookkeeperJdbc(db: Database) extends BookkeeperBase(true) {

  import za.co.absa.pramen.core.utils.FutureImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  override val bookkeepingEnabled: Boolean = true

  override def getLatestProcessedDateFromStorage(table: String, until: Option[LocalDate]): Option[LocalDate] = {
    val query = until match {
      case Some(endDate) =>
        val endDateStr = DataChunk.dateFormatter.format(endDate)
        BookkeepingRecords.records
          .filter(r => r.pramenTableName === table && r.infoDate <= endDateStr)
      case None          =>
        BookkeepingRecords.records
          .filter(r => r.pramenTableName === table)
    }

    val chunks = try {
      SlickUtils.executeQuery[BookkeepingRecords, BookkeepingRecord](db, query)
        .map(toChunk)
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

  override def getLatestDataChunkFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Option[DataChunk] = {
    getDataChunksFromStorage(table, dateBegin, dateEnd).lastOption
  }

  override def getDataChunksFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataChunk] = {
    val query = getFilter(table, Option(dateBegin), Option(dateEnd))

    try {
      SlickUtils.executeQuery[BookkeepingRecords, BookkeepingRecord](db, query)
        .map(toChunk)
        .sortBy(_.jobFinished)
        .groupBy(v => (v.tableName, v.infoDate))
        .map { case (_, listChunks) =>
          listChunks.maxBy(c => c.jobFinished)
        }
        .toArray[DataChunk]
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the bookkeeping table.", ex)
    }
  }

  def getDataChunksCountFromStorage(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long = {
    val query = getFilter(table, dateBeginOpt, dateEndOpt)
      .length

    val count = try {
      SlickUtils.executeCount(db, query)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the bookkeeping table.", ex)
    }

    count
  }

  private[pramen] override def saveRecordCountToStorage(table: String,
                                                        infoDate: LocalDate,
                                                        infoDateBegin: LocalDate,
                                                        infoDateEnd: LocalDate,
                                                        inputRecordCount: Long,
                                                        outputRecordCount: Long,
                                                        jobStarted: Long,
                                                        jobFinished: Long): Unit = {
    val dateStr = DataChunk.dateFormatter.format(infoDate)
    val dateBeginStr = DataChunk.dateFormatter.format(infoDateBegin)
    val dateEndStr = DataChunk.dateFormatter.format(infoDateEnd)

    val record = BookkeepingRecord(table, dateStr, dateBeginStr, dateEndStr, inputRecordCount, outputRecordCount, jobStarted, jobFinished)

    try {
      db.run(
        BookkeepingRecords.records += record
      ).execute()
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to write to the bookkeeping table.", ex)
    }
  }

  private def toChunk(r: BookkeepingRecord): DataChunk = {
    DataChunk(
      r.pramenTableName, r.infoDate, r.infoDateBegin, r.infoDateEnd, r.inputRecordCount, r.outputRecordCount, r.jobStarted, r.jobFinished)
  }

  private def getFilter(tableName: String, infoDateBeginOpt: Option[LocalDate], infoDateEndOpt: Option[LocalDate]): Query[BookkeepingRecords, BookkeepingRecord, Seq] = {
    (infoDateBeginOpt, infoDateEndOpt) match {
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
  }

  override def getLatestSchema(table: String, infoDate: LocalDate): Option[(StructType, LocalDate)] = {
    val infoDateStr = infoDate.toString
    val query = SchemaRecords.records.filter(t => t.pramenTableName === table && t.infoDate <= infoDateStr)
      .sortBy(t => t.infoDate.desc)

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
  def fromJdbcConfig(jdbcConfig: JdbcConfig): BookkeeperJdbc = {
    val selector = JdbcUrlSelector(jdbcConfig)
    val url = selector.getWorkingUrl(DEFAULT_RETRIES)
    val prop = selector.getProperties

    val db = if (jdbcConfig.user.nonEmpty) {
      Database.forURL(url = url, driver = jdbcConfig.driver, user = jdbcConfig.user.get, password = jdbcConfig.password.getOrElse(""), prop = prop)
    } else {
      Database.forURL(url = url, driver = jdbcConfig.driver, prop = prop)
    }
    new BookkeeperJdbc(db)
  }

  def fromConfig(conf: Config, path: String): BookkeeperJdbc = {
    val db = Database.forConfig(path, conf)
    new BookkeeperJdbc(db)
  }

}
