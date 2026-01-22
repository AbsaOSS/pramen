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

import za.co.absa.pramen.core.bookkeeper.model.DataAvailability
import za.co.absa.pramen.core.model.DataChunk

import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

abstract class BookkeeperBase(isBookkeepingEnabled: Boolean, batchId: Long) extends Bookkeeper {
  private val transientDataChunks = new mutable.HashMap[String, Array[DataChunk]]()

  def getLatestProcessedDateFromStorage(table: String, until: Option[LocalDate] = None): Option[LocalDate]

  def getLatestDataChunkFromStorage(table: String, infoDate: LocalDate): Option[DataChunk]

  def getDataChunksFromStorage(table: String, infoDate: LocalDate, batchId: Option[Long]): Seq[DataChunk]

  def getDataChunksCountFromStorage(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long

  def getDataAvailabilityFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataAvailability]

  def deleteNonCurrentBatchRecords(table: String, infoDate: LocalDate): Unit

  private[pramen] def saveRecordCountToStorage(table: String,
                                               infoDate: LocalDate,
                                               inputRecordCount: Long,
                                               outputRecordCount: Long,
                                               recordsAppended: Option[Long],
                                               jobStarted: Long,
                                               jobFinished: Long): Unit

  private[pramen] final override def setRecordCount(table: String,
                                           infoDate: LocalDate,
                                           inputRecordCount: Long,
                                           outputRecordCount: Long,
                                           recordsAppended: Option[Long],
                                           jobStarted: Long,
                                           jobFinished: Long,
                                           isTableTransient: Boolean): Unit = {
    if (isTableTransient || !isBookkeepingEnabled) {
      val tableLowerCase = table.toLowerCase
      val dataChunk = DataChunk(table, infoDate.toString, infoDate.toString, infoDate.toString, inputRecordCount, outputRecordCount, jobStarted, jobFinished, Option(batchId), recordsAppended)
      this.synchronized {
        val dataChunks = transientDataChunks.getOrElse(tableLowerCase, Array.empty[DataChunk])
        val newDataChunks = (dataChunks :+ dataChunk).sortBy(_.jobFinished)
        transientDataChunks += tableLowerCase -> newDataChunks
      }
    } else {
      saveRecordCountToStorage(table, infoDate, inputRecordCount, outputRecordCount, recordsAppended, jobStarted, jobFinished)
      if (recordsAppended.isEmpty) {
        deleteNonCurrentBatchRecords(table, infoDate)
      }
    }
  }

  final override def getLatestProcessedDate(table: String, until: Option[LocalDate] = None): Option[LocalDate] = {
    val isTransient = this.synchronized {
      transientDataChunks.contains(table.toLowerCase)
    }

    if (isTransient || !isBookkeepingEnabled) {
      getLatestTransientDate(table, None, until)
    } else {
      getLatestProcessedDateFromStorage(table, until)
    }
  }


  final def getLatestDataChunk(table: String, infoDate: LocalDate): Option[DataChunk] = {
    val isTransient = this.synchronized {
      transientDataChunks.contains(table.toLowerCase)
    }

    if (isTransient || !isBookkeepingEnabled) {
      getLatestTransientChunk(table, Option(infoDate), Option(infoDate))
    } else {
      getLatestDataChunkFromStorage(table, infoDate)
    }
  }

  final override def getDataChunks(table: String, infoDate: LocalDate, batchId: Option[Long]): Seq[DataChunk] = {
    val isTransient = this.synchronized {
      transientDataChunks.contains(table.toLowerCase)
    }

    if (isTransient || !isBookkeepingEnabled) {
      getTransientDataChunks(table, Option(infoDate), Option(infoDate), batchId)
    } else {
      getDataChunksFromStorage(table, infoDate, batchId)
    }
  }

  final override def getDataChunksCount(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long = {
    val isTransient = this.synchronized {
      transientDataChunks.contains(table.toLowerCase)
    }

    if (isTransient || !isBookkeepingEnabled) {
      getTransientDataChunks(table, dateBeginOpt, dateEndOpt, None).length
    } else {
      getDataChunksCountFromStorage(table, dateBeginOpt, dateEndOpt)
    }
  }

  final override def getDataAvailability(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataAvailability] = {
    if (dateBegin.isAfter(dateEnd)) return Seq.empty

    val isTransient = this.synchronized {
      transientDataChunks.contains(table.toLowerCase)
    }

    if (isTransient || !isBookkeepingEnabled) {
      getDataAvailabilityTransient(table, dateBegin, dateEnd)
    } else {
      getDataAvailabilityFromStorage(table, dateBegin, dateEnd)
    }
  }

  private[pramen] override def getOffsetManager: OffsetManager = {
    throw new IllegalArgumentException(s"This implementation of bookkeeping does not support offset management and incremental pipelines. " +
      "Please, use JDBC for bookkeeping to enable this.")
  }

  private def getLatestTransientDate(table: String, from: Option[LocalDate], until: Option[LocalDate]): Option[LocalDate] = {
    val chunks = getTransientDataChunks(table, from, until, None)

    if (chunks.isEmpty) {
      None
    } else {
      Option (
        LocalDate.parse(chunks.maxBy(_.infoDate).infoDate)
      )
    }
  }

  private def getLatestTransientChunk(table: String, from: Option[LocalDate], until: Option[LocalDate]): Option[DataChunk] = {
    getTransientDataChunks(table, from, until, None)
      .lastOption
  }


  private[core] def getTransientDataChunks(table: String, from: Option[LocalDate], until: Option[LocalDate], batchId: Option[Long]): Array[DataChunk] = {
    val minDate = from.map(_.toString).getOrElse("0000-00-00")
    val maxDate = until.map(_.toString).getOrElse("9999-99-99")
    val tableLowerCase = table.toLowerCase
    val allChunks = this.synchronized {
      transientDataChunks.getOrElse(tableLowerCase, Array.empty[DataChunk])
    }

    batchId match {
      case Some(id) =>
        allChunks.filter(chunk => chunk.infoDate >= minDate && chunk.infoDate <= maxDate && chunk.batchId.contains(id))
      case None =>
        allChunks.filter(chunk => chunk.infoDate >= minDate && chunk.infoDate <= maxDate)
    }
  }

  private[core] def getDataAvailabilityTransient(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataAvailability] = {
    if (dateBegin.isAfter(dateEnd)) return Seq.empty
    val dateEndPlus = dateEnd.plusDays(1)
    var date = dateBegin

    val foundDataAvailable = new ListBuffer[DataAvailability]

    while (date.isBefore(dateEndPlus)) {
      val chunks = getTransientDataChunks(table, Option(date), Option(date), None)

      if (chunks.nonEmpty) {
        val totalRecords = chunks.map(_.outputRecordCount).sum
        foundDataAvailable += DataAvailability(date, chunks.length, totalRecords)
      }

      date = date.plusDays(1)
    }

    foundDataAvailable.toSeq
  }

  protected def getDateStr(date: LocalDate): String = DataChunk.dateFormatter.format(date)
}
