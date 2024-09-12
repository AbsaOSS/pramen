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

import org.slf4j.LoggerFactory
import slick.jdbc.H2Profile.api._
import za.co.absa.pramen.api.offset.{DataOffset, OffsetValue}
import za.co.absa.pramen.core.bookkeeper.model._
import za.co.absa.pramen.core.utils.SlickUtils

import java.time.{Instant, LocalDate}
import scala.util.control.NonFatal

class OffsetManagerJdbc(db: Database) extends OffsetManager {
  import za.co.absa.pramen.core.utils.FutureImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  override def getOffsets(table: String, infoDate: LocalDate): Array[DataOffset] = {
    val offsets = getOffsetRecords(table, infoDate)

    if (offsets.isEmpty) {
      return Array.empty
    }

    offsets.map(OffsetRecordConverter.toDataOffset)
  }

  override def getMaxInfoDateAndOffset(table: String, onlyForInfoDate: Option[LocalDate]): Option[DataOffsetAggregated] = {
    val maxInfoDateOpt = onlyForInfoDate.orElse(getMaximumInfoDate(table))

    try {
      maxInfoDateOpt.flatMap { infoDate =>
        getMinMaxOffsets(table, infoDate)
      }
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the offset table.", ex)
    }
  }

  override def startWriteOffsets(table: String, infoDate: LocalDate, minOffset: OffsetValue): DataOffsetRequest = {
    val createdAt = Instant.now().toEpochMilli

    val record = OffsetRecord(table, infoDate.toString, minOffset.dataTypeString, minOffset.valueString, "", createdAt, None)

    db.run(
      OffsetRecords.records += record
    ).execute()

    DataOffsetRequest(table, infoDate, minOffset, createdAt)
  }

  override def commitOffsets(request: DataOffsetRequest, maxOffset: OffsetValue): Unit = {
    val committedAt = Instant.now().toEpochMilli

    db.run(
      OffsetRecords.records
        .filter(r => r.pramenTableName === request.tableName && r.infoDate === request.infoDate.toString && r.createdAt === request.createdAt)
        .map(r => (r.maxOffset, r.committedAt))
        .update((maxOffset.valueString, Some(committedAt)))
    ).execute()
  }

  override def rollbackOffsets(request: DataOffsetRequest): Unit = {
    db.run(
      OffsetRecords.records
        .filter(r => r.pramenTableName === request.tableName && r.infoDate === request.infoDate.toString && r.createdAt === request.createdAt)
        .delete
    ).execute()
  }

  private[core] def getMaximumInfoDate(table: String): Option[LocalDate] = {
    val query = OffsetRecords.records
      .filter(r => r.pramenTableName === table)
      .map(_.infoDate).max

    try {
      SlickUtils.executeMaxString(db, query)
        .map(LocalDate.parse)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Unable to read from the offset table.", ex)
    }
  }

  private[core] def getOffsetRecords(table: String, infoDate: LocalDate): Array[OffsetRecord] = {
    val infoDateStr = infoDate.toString
    val query = OffsetRecords.records
      .filter(r => r.pramenTableName === table && r.infoDate === infoDateStr)

    SlickUtils.executeQuery[OffsetRecords, OffsetRecord](db, query)
      .toArray[OffsetRecord]
  }

  private[core] def getMinMaxOffsets(table: String, infoDate: LocalDate): Option[DataOffsetAggregated] = {
    val offsets = getOffsetRecords(table, infoDate).filter(_.committedAtMilli.nonEmpty)

    if (offsets.isEmpty) {
      return None
    }

    validateOffsets(table, infoDate, offsets)

    val offsetDataType =  offsets.head.dataType
    val minOffset = OffsetValue.fromString(offsetDataType, offsets.map(_.minOffset).min)
    val maxOffset = OffsetValue.fromString(offsetDataType, offsets.map(_.maxOffset).max)

    Some(DataOffsetAggregated(table, infoDate, minOffset, maxOffset, offsets.map(OffsetRecordConverter.toDataOffset)))
  }

  /**
    * Checks offsets for inconsistencies. They include:
    * - inconsistent offset value types
    *
    * @param offsets An array of offset records
    */
  private[core] def validateOffsets(table: String, infoDate: LocalDate, offsets: Array[OffsetRecord]): Unit = {
    val inconsistentOffsets = offsets.groupBy(_.dataType).keys.toArray.sorted
    if (inconsistentOffsets.length > 1) {
      throw new RuntimeException(s"Inconsistent offset value types found for $table at $infoDate: ${inconsistentOffsets.mkString(", ")}")
    }
  }

}
