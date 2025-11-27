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
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.MetadataManager
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.model.{ReaderMode, TrackingTable}

import java.time.{Instant, LocalDate}
import scala.collection.mutable.ListBuffer

class MetastoreReaderIncrementalImpl(metastore: Metastore,
                                     metadataManager: MetadataManager,
                                     bookkeeper: Bookkeeper,
                                     tables: Seq[String],
                                     outputTable: String,
                                     infoDate: LocalDate,
                                     runReason: TaskRunReason,
                                     readMode: ReaderMode,
                                     isRerun: Boolean)
  extends MetastoreReaderBase(metastore, metadataManager, bookkeeper, tables, infoDate, runReason)
    with MetastoreReaderIncremental {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val trackingTables = new ListBuffer[TrackingTable]

  override def getCurrentBatch(tableName: String): DataFrame = {
    validateTable(tableName)
    if (readMode == ReaderMode.IncrementalPostProcessing && !isRerun) {
      log.info(s"Getting the current batch for table '$tableName' at '$infoDate'...")
      metastore.getBatch(tableName, infoDate, None)
    } else if (readMode == ReaderMode.IncrementalValidation || readMode == ReaderMode.IncrementalRun) {
      if (isRerun) {
        log.info(s"Getting the current incremental chunk for table rerun '$tableName' at '$infoDate'...")
        getIncrementalForRerun(tableName, infoDate)
      } else {
        log.info(s"Getting the current incremental chunk for table '$tableName' at '$infoDate'...")
        getIncremental(tableName, infoDate)
      }
    } else {
      log.info(s"Getting daily data for table '$tableName' at '$infoDate'...")
      metastore.getTable(tableName, Option(infoDate), Option(infoDate))
    }
  }

  override def commitIncrementalOutputTable(tableName: String, trackingName: String): Unit = {
    if (readMode != ReaderMode.Batch) {
      val om = bookkeeper.getOffsetManager
      val minMax = om.getMaxInfoDateAndOffset(trackingName, Option(infoDate))
      log.info(s"Starting offset commit for output table '$trackingName' for '$infoDate'.")
      val trackingTable = TrackingTable(
        Thread.currentThread().getId,
        tableName,
        outputTable,
        trackingName,
        "",
        minMax.map(_.minimumOffset),
        minMax.map(_.maximumOffset),
        infoDate,
        Instant.now()
      )

      trackingTables += trackingTable
    }
  }

  override def commitIncrementalStage(): Unit = {
    metastore.addTrackingTables(trackingTables.toSeq)
    trackingTables.clear()
  }

  private def getIncremental(tableName: String, infoDate: LocalDate): DataFrame = {
    val commitChanges = readMode == ReaderMode.IncrementalRun
    val trackingName = s"$tableName->$outputTable"

    getIncrementalDf(tableName, trackingName, infoDate, commitChanges)
  }

  private def getIncrementalForRerun(tableName: String, infoDate: LocalDate): DataFrame = {
    val commitChanges = readMode == ReaderMode.IncrementalRun
    val trackingName = s"$tableName->$outputTable"

    getIncrementalDfForRerun(tableName, trackingName, infoDate, commitChanges)
  }

  private def getIncrementalDf(tableName: String, trackingName: String, infoDate: LocalDate, commit: Boolean): DataFrame = {
    val tableDef = metastore.getTableDef(tableName)
    val om = bookkeeper.getOffsetManager
    val tableDf = metastore.getTable(tableName, Option(infoDate), Option(infoDate))
    val offsets = om.getMaxInfoDateAndOffset(trackingName, Option(infoDate))

    val df = if (tableDf.isEmpty) {
      tableDf
    } else {
      if (!tableDf.schema.exists(_.name == tableDef.batchIdColumn)) {
        log.error(tableDf.schema.treeString)
        throw new IllegalArgumentException(s"Table '$tableName' does not contain column '${tableDef.batchIdColumn}' needed for incremental processing.")
      }

      offsets match {
        case Some(values) =>
          log.info(s"Getting incremental table '$trackingName' for '$infoDate', column '${tableDef.batchIdColumn}' > ${values.maximumOffset.valueString}")
          tableDf.filter(col(tableDef.batchIdColumn) > values.maximumOffset.getSparkLit)
        case None =>
          log.info(s"Getting incremental table '$trackingName' for '$infoDate''")
          tableDf
      }
    }

    if (commit && !trackingTables.exists(t => t.trackingName == trackingName && t.infoDate == infoDate)) {
      log.info(s"Starting offset commit for table '$trackingName' for '$infoDate'")

      val trackingTable = TrackingTable(
        Thread.currentThread().getId,
        tableName,
        outputTable,
        trackingName,
        tableDef.batchIdColumn,
        offsets.map(_.minimumOffset),
        offsets.map(_.maximumOffset),
        infoDate,
        Instant.now()
      )

      trackingTables += trackingTable
    }

    df
  }

  private def getIncrementalDfForRerun(tableName: String, trackingName: String, infoDate: LocalDate, commit: Boolean): DataFrame = {
    val tableDef = metastore.getTableDef(tableName)
    val om = bookkeeper.getOffsetManager
    val offsets = om.getMaxInfoDateAndOffset(trackingName, Option(infoDate))
    val tableDf = metastore.getTable(tableName, Option(infoDate), Option(infoDate))

    if (commit && !trackingTables.exists(t => t.trackingName == trackingName && t.infoDate == infoDate)) {
      log.info(s"Starting offset commit for table rerun '$trackingName' for '$infoDate'")

      val trackingTable = TrackingTable(
        Thread.currentThread().getId,
        tableName,
        outputTable,
        trackingName,
        tableDef.batchIdColumn,
        offsets.map(_.minimumOffset),
        offsets.map(_.maximumOffset),
        infoDate,
        Instant.now()
      )

      trackingTables += trackingTable
    }

    tableDf
  }
}
