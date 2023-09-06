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

package za.co.absa.pramen.core.mocks.bookkeeper

import org.apache.spark.sql.types.{DataType, StructType}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.model.{DataChunk, TableSchema}

import java.time.LocalDate
import scala.collection.mutable
import scala.util.Try

class SyncBookkeeperMock extends Bookkeeper {
  private val chunks = new mutable.HashMap[(String, LocalDate), DataChunk]()
  private val schemas = new mutable.ListBuffer[(String, (LocalDate, TableSchema))]()

  def clear(): Unit = chunks.clear()

  def getChunks: Seq[DataChunk] = chunks.toList.map(_._2)

  override val bookkeepingEnabled: Boolean = true

  override def getLatestProcessedDate(table: String, until: Option[LocalDate]): Option[LocalDate] = {
    val c = until match {
      case None       =>
        chunks.toList.filter { case ((tblName, _), _) => tblName == table }.map(_._2)
      case Some(date) =>
        chunks.toList.filter { case ((tblName, infoDate), _) => tblName == table &&
          (infoDate.equals(date) || infoDate.isBefore(date))
        }.map(_._2)
    }

    if (c.isEmpty) {
      None
    } else {
      val chunk = c.maxBy(_.infoDate)
      Option(LocalDate.parse(chunk.infoDate, DataChunk.dateFormatter))
    }
  }

  override def getLatestDataChunk(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Option[DataChunk] = {
    getDataChunks(table, dateBegin, dateEnd).lastOption
  }

  override def getDataChunks(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Seq[DataChunk] = {
    chunks.toList.flatMap { case ((tblName, infoDate), chunk) =>
      val isInsidePeriod = tblName == table && (infoDate.isAfter(dateBegin) || infoDate.equals(dateBegin)) &&
        (infoDate.isBefore(dateEnd) || infoDate.equals(dateEnd))
      if (isInsidePeriod) {
        Some(chunk)
      } else {
        None
      }
    }.sortBy(_.jobFinished)
  }

  override def getDataChunksCount(table: String, dateBeginOpt: Option[LocalDate], dateEndOpt: Option[LocalDate]): Long = {
    chunks.toList.flatMap { case ((tblName, infoDate), chunk) =>
      val isBeforeConditionHolds = dateBeginOpt.forall(d => infoDate.isAfter(d) || infoDate.equals(d))
      val isUntilConditionHolds = dateEndOpt.forall(d => infoDate.isBefore(d) || infoDate.equals(d))

      val isInsidePeriod = tblName == table && isBeforeConditionHolds && isUntilConditionHolds

      if (isInsidePeriod) {
        Some(chunk)
      } else {
        None
      }
    }.size
  }

  private[pramen] override def setRecordCount(table: String,
                                               infoDate: LocalDate,
                                               infoDateBegin: LocalDate,
                                               infoDateEnd: LocalDate,
                                               inputRecordCount: Long,
                                               outputRecordCount: Long,
                                               jobStarted: Long,
                                               jobFinished: Long,
                                               isTableTransient: Boolean): Unit = {
    val dateStr = DataChunk.dateFormatter.format(infoDate)
    val dateBeginStr = DataChunk.dateFormatter.format(infoDateBegin)
    val dateEndStr = DataChunk.dateFormatter.format(infoDateEnd)

    val chunk = DataChunk(table,
      dateStr,
      dateBeginStr,
      dateEndStr,
      inputRecordCount,
      outputRecordCount,
      jobStarted,
      jobFinished)

    chunks += (table, infoDate) -> chunk
  }

  override def getLatestSchema(table: String, infoDate: LocalDate): Option[(StructType, LocalDate)] = {
    schemas.filter(_._1 == table).map(_._2)
      .flatMap { case (date, tableSchema) =>
        if (infoDate.isBefore(date)) {
          None
        } else {
          Try {
            (DataType.fromJson(tableSchema.schemaJson).asInstanceOf[StructType], LocalDate.parse(tableSchema.infoDate))
          }.toOption
        }
      }.sortBy(a => - a._2.toEpochDay)
      .headOption
  }

  override def saveSchema(table: String, infoDate: LocalDate, schema: StructType): Unit = {
    val tableSchema = TableSchema(
      table,
      infoDate.toString,
      schema.json
    )

    schemas += table -> (infoDate, tableSchema)
  }
}
