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

import za.co.absa.pramen.core.model.DataChunk

import java.time.LocalDate
import scala.collection.mutable

abstract class BookkeeperBase(isBookkeepingEnabled: Boolean) extends Bookkeeper {
  private val transientDataChunks = new mutable.HashMap[String, Array[DataChunk]]()

  def getLatestProcessedDateFromStorage(table: String, until: Option[LocalDate] = None): Option[LocalDate]

  def getLatestDataChunkFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Option[DataChunk]

  def getDataChunksFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataChunk]

  def getDataChunksCountFromStorage(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long

  private[pramen] def saveRecordCountToStorage(table: String,
                                               infoDate: LocalDate,
                                               infoDateBegin: LocalDate,
                                               infoDateEnd: LocalDate,
                                               inputRecordCount: Long,
                                               outputRecordCount: Long,
                                               jobStarted: Long,
                                               jobFinished: Long): Unit

  private[pramen] final def setRecordCount(table: String,
                                           infoDate: LocalDate,
                                           infoDateBegin: LocalDate,
                                           infoDateEnd: LocalDate,
                                           inputRecordCount: Long,
                                           outputRecordCount: Long,
                                           jobStarted: Long,
                                           jobFinished: Long,
                                           isTableTransient: Boolean): Unit = {
    if (isTableTransient || !isBookkeepingEnabled) {
      val tableLowerCase = table.toLowerCase
      val dataChunk = DataChunk(table, infoDate.toString, infoDateBegin.toString, infoDateEnd.toString, inputRecordCount, outputRecordCount, jobStarted, jobFinished)
      this.synchronized {
        val dataChunks = transientDataChunks.getOrElse(tableLowerCase, Array.empty[DataChunk])
        val newDataChunks = (dataChunks :+ dataChunk).sortBy(_.jobFinished)
        transientDataChunks += tableLowerCase -> newDataChunks
      }
    } else {
      saveRecordCountToStorage(table, infoDate, infoDateBegin, infoDateEnd, inputRecordCount, outputRecordCount, jobStarted, jobFinished)
    }
  }

  final def getLatestProcessedDate(table: String, until: Option[LocalDate] = None): Option[LocalDate] = {
    val isTransient = this.synchronized {
      transientDataChunks.contains(table.toLowerCase)
    }

    if (isTransient || !isBookkeepingEnabled) {
      getLatestTransientDate(table, None, until)
    } else {
      getLatestProcessedDateFromStorage(table, until)
    }
  }


  final def getLatestDataChunk(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Option[DataChunk] = {
    val isTransient = this.synchronized {
      transientDataChunks.contains(table.toLowerCase)
    }

    if (isTransient || !isBookkeepingEnabled) {
      getLatestTransientChunk(table, Option(dateBegin), Option(dateEnd))
    } else {
      getLatestDataChunkFromStorage(table, dateBegin, dateEnd)
    }
  }

  final def getDataChunks(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataChunk] = {
    val isTransient = this.synchronized {
      transientDataChunks.contains(table.toLowerCase)
    }

    if (isTransient || !isBookkeepingEnabled) {
      getTransientDataChunks(table, Option(dateBegin), Option(dateEnd))
    } else {
      getDataChunksFromStorage(table, dateBegin, dateEnd)
    }
  }

  final def getDataChunksCount(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long = {
    val isTransient = this.synchronized {
      transientDataChunks.contains(table.toLowerCase)
    }

    if (isTransient || !isBookkeepingEnabled) {
      getTransientDataChunks(table, dateBeginOpt, dateEndOpt).length
    } else {
      getDataChunksCountFromStorage(table, dateBeginOpt, dateEndOpt)
    }
  }

  private def getLatestTransientDate(table: String, from: Option[LocalDate], until: Option[LocalDate]): Option[LocalDate] = {
    val chunks = getTransientDataChunks(table, from, until)

    if (chunks.isEmpty) {
      None
    } else {
      Option (
        LocalDate.parse(chunks.maxBy(_.infoDate).infoDate)
      )
    }
  }

  private def getLatestTransientChunk(table: String, from: Option[LocalDate], until: Option[LocalDate]): Option[DataChunk] = {
    getTransientDataChunks(table, from, until)
      .lastOption
  }


  private def getTransientDataChunks(table: String, from: Option[LocalDate], until: Option[LocalDate]): Array[DataChunk] = {
    val minDate = from.map(_.toString).getOrElse("0000-00-00")
    val maxDate = until.map(_.toString).getOrElse("9999-99-99")
    val tableLowerCase = table.toLowerCase
    val allChunks = this.synchronized {
      transientDataChunks.getOrElse(tableLowerCase, Array.empty[DataChunk])
    }

    allChunks.filter(chunk => chunk.infoDate >= minDate && chunk.infoDate <= maxDate)
  }

  protected def getDateStr(date: LocalDate): String = DataChunk.dateFormatter.format(date)
}
