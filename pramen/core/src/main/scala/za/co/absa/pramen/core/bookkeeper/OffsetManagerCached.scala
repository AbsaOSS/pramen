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
import za.co.absa.pramen.api.offset.DataOffset.UncommittedOffset
import za.co.absa.pramen.api.offset.{DataOffset, OffsetType, OffsetValue}
import za.co.absa.pramen.core.bookkeeper.model.{DataOffsetAggregated, DataOffsetRequest}

import java.time.LocalDate
import scala.collection.mutable

/**
  * The offset manager decorator handles caching or repeated queries.
  */
class OffsetManagerCached(offsetManager: OffsetManager) extends OffsetManager {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val aggregatedOffsetsCache = new mutable.HashMap[(String, Option[LocalDate]), Option[DataOffsetAggregated]]

  def getOffsets(table: String, infoDate: LocalDate): Array[DataOffset] = {
    offsetManager.getOffsets(table, infoDate)
  }

  def getUncommittedOffsets(table: String, onlyForInfoDate: Option[LocalDate]): Array[UncommittedOffset] = {
    offsetManager.getUncommittedOffsets(table, onlyForInfoDate)
  }

  def getMaxInfoDateAndOffset(table: String, onlyForInfoDate: Option[LocalDate]): Option[DataOffsetAggregated] = synchronized {
    if (aggregatedOffsetsCache.contains((table, onlyForInfoDate))) {
      log.info(s"Got min/max offsets for '$table' from cache.")
      aggregatedOffsetsCache((table, onlyForInfoDate))
    } else {
      val value = offsetManager.getMaxInfoDateAndOffset(table, onlyForInfoDate)
      log.info(s"Got min/max offsets for '$table' from the database. Saving to cache...")
      aggregatedOffsetsCache += (table, onlyForInfoDate) -> value
      value
    }
  }

  def startWriteOffsets(table: String, infoDate: LocalDate, offsetType: OffsetType): DataOffsetRequest = {
    offsetManager.startWriteOffsets(table, infoDate, offsetType)
  }

  def commitOffsets(request: DataOffsetRequest, minOffset: OffsetValue, maxOffset: OffsetValue): Unit = {
    offsetManager.commitOffsets(request, minOffset, maxOffset)

    this.synchronized {
      aggregatedOffsetsCache --= aggregatedOffsetsCache.keys.filter(_._1 == request.tableName)
    }
  }

  def commitRerun(request: DataOffsetRequest, minOffset: OffsetValue, maxOffset: OffsetValue): Unit = {
    this.synchronized {
      aggregatedOffsetsCache --= aggregatedOffsetsCache.keys.filter(_._1 == request.tableName)
    }

    offsetManager.commitRerun(request, minOffset, maxOffset)
  }

  def postCommittedRecords(commitRequests: Seq[OffsetCommitRequest]): Unit = {
    offsetManager.postCommittedRecords(commitRequests)

    val updatedTables = commitRequests.map(_.table).toSet
    this.synchronized {
      aggregatedOffsetsCache --= aggregatedOffsetsCache.keys.filter(k => updatedTables.contains(k._1))
    }
  }

  def rollbackOffsets(request: DataOffsetRequest): Unit = {
    offsetManager.rollbackOffsets(request)
  }
}
