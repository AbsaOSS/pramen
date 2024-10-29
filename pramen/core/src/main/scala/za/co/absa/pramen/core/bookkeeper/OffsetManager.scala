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

import za.co.absa.pramen.api.offset.DataOffset.UncommittedOffset
import za.co.absa.pramen.api.offset.{DataOffset, OffsetType, OffsetValue}
import za.co.absa.pramen.core.bookkeeper.model.{DataOffsetAggregated, DataOffsetRequest}

import java.time.LocalDate

/**
  * The offset manager allows managing offsets of incremental operations.
  *
  * It provides correctness guarantees only if not running in parallel with other jobs outputting to the same
  * Pramen table.
  *
  * Locks are used to ensure no jobs outputting to the same incremental table is happening in parallel.
  *
  * The startWriteOffsets() together with commitOffsets() and rollbackOffsets() provide mechanisms to ensure consistency
  * with data.
  */
trait OffsetManager {
  /**
    * Returns offsets for an information date.
    *
    * If there are uncommitted offsets, they should be handled this way:
    * - maximum offset should be derived from data,
    * - the latest uncommitted offset should be committed to the latest offset
    * - previous uncommitted offsets should be rolled back.
    */
  def getOffsets(table: String, infoDate: LocalDate): Array[DataOffset]

  /**
    * Returns only uncommitted offsets for a give table.
    */
  def getUncommittedOffsets(table: String, onlyForInfoDate: Option[LocalDate]): Array[UncommittedOffset]

  /**
    * Returns the maximum information date the bookkeeping has offsets for.
    *
    * If onlyForInfoDate is not empty, offset management is being processed for each info date individually, e.g.
    * info_date + offset column is monotonic.
    *
    * if onlyForInfoDate is empty
    * offset must be a monotonically increasing field.
    */
  def getMaxInfoDateAndOffset(table: String, onlyForInfoDate: Option[LocalDate]): Option[DataOffsetAggregated]

  /**
    * Starts an uncommitted offset for an incremental ingestion for a day.
    * This can only be done for the latest information date.
    */
  def startWriteOffsets(table: String, infoDate: LocalDate, offsetType: OffsetType): DataOffsetRequest

  /**
    * Commits changes to the table. If maxOffset is
    * - greater than or equal to minOffset, a new entry is created.
    * - less than minOffset - an exception will be thrown.
    */
  def commitOffsets(request: DataOffsetRequest, minOffset: OffsetValue, maxOffset: OffsetValue): Unit

  /**
    * Commits changes to the table as the result of a re-run. This replaces all batch ids
    * and offsets for that day with new batch id.
    */
  def commitRerun(request: DataOffsetRequest, minOffset: OffsetValue, maxOffset: OffsetValue): Unit

  /**
    * Rolls back an offset request
    */
  def rollbackOffsets(request: DataOffsetRequest): Unit
}
