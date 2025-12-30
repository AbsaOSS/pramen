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

package za.co.absa.pramen.core.metastore

import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.offset.DataOffset
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.api.{MetaTableDef, MetaTableRunInfo, MetadataManager, MetastoreReader}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.model.MetaTable

import java.time.{Instant, LocalDate}

abstract class MetastoreReaderBase(metastore: Metastore,
                                   metadata: MetadataManager,
                                   bookkeeper: Bookkeeper,
                                   tables: Seq[String],
                                   infoDate: LocalDate,
                                   val batchId: Long,
                                   runReason: TaskRunReason) extends MetastoreReader {
  override def getTable(tableName: String, infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    validateTable(tableName)
    val from = infoDateFrom.orElse(Option(infoDate))
    val to = infoDateTo.orElse(Option(infoDate))
    metastore.getTable(tableName, from, to)
  }

  override def getLatest(tableName: String, until: Option[LocalDate]): DataFrame = {
    validateTable(tableName)
    val untilDate = until.orElse(Option(infoDate))
    metastore.getLatest(tableName, untilDate)
  }

  override def getLatestAvailableDate(tableName: String, until: Option[LocalDate]): Option[LocalDate] = {
    validateTable(tableName)
    val untilDate = until.orElse(Option(infoDate))
    bookkeeper.getLatestProcessedDate(tableName, untilDate)
  }

  override def isDataAvailable(tableName: String, from: Option[LocalDate], until: Option[LocalDate]): Boolean = {
    validateTable(tableName)
    val fromDate = from.orElse(Option(infoDate))
    val untilDate = until.orElse(Option(infoDate))
    metastore.isDataAvailable(tableName, fromDate, untilDate)
  }

  override def getOffsets(table: String, infoDate: LocalDate): Array[DataOffset] = {
    val om = bookkeeper.getOffsetManager

    om.getOffsets(table, infoDate)
  }

  override def getTableDef(tableName: String): MetaTableDef = {
    validateTable(tableName)

    MetaTable.getMetaTableDef(metastore.getTableDef(tableName))
  }

  override def getTableRunInfo(tableName: String, infoDate: LocalDate, batchId: Option[Long]): Seq[MetaTableRunInfo] = {
    bookkeeper.getDataChunks(tableName, infoDate, batchId)
      .map(chunk =>
        MetaTableRunInfo(
          tableName,
          LocalDate.parse(chunk.infoDate),
          chunk.batchId,
          chunk.inputRecordCount,
          chunk.outputRecordCount,
          Instant.ofEpochSecond(chunk.jobStarted),
          Instant.ofEpochSecond(chunk.jobFinished)
        )
      )
  }

  override def getRunReason: TaskRunReason = {
    runReason
  }

  override def metadataManager: MetadataManager = metadata

  protected def validateTable(tableName: String): Unit = {
    if (!tables.contains(tableName)) {
      throw new TableNotConfigured(s"Attempt accessing non-dependent table: $tableName")
    }
  }
}
