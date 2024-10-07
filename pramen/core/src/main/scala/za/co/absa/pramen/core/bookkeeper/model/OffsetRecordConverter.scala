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

package za.co.absa.pramen.core.bookkeeper.model

import za.co.absa.pramen.api.offset.{DataOffset, OffsetType, OffsetValue}

import java.time.{Instant, LocalDate}

object OffsetRecordConverter {
  def toDataOffset(r: OffsetRecord): DataOffset = {
    val minOffsetOpt = OffsetValue.fromString(r.dataType, r.minOffset)
    val maxOffsetOpt = OffsetValue.fromString(r.dataType, r.maxOffset)

    if (r.committedAtMilli.nonEmpty && minOffsetOpt.isDefined && maxOffsetOpt.isDefined) {
      DataOffset.CommittedOffset(
        r.pramenTableName,
        LocalDate.parse(r.infoDate),
        r.batchId,
        minOffsetOpt.get,
        maxOffsetOpt.get,
        Instant.ofEpochMilli(r.createdAtMilli),
        Instant.ofEpochMilli(r.committedAtMilli.get)
      )
    } else {
      val dataType = OffsetType.fromString(r.dataType)

      DataOffset.UncommittedOffset(
        r.pramenTableName,
        LocalDate.parse(r.infoDate),
        r.batchId,
        dataType,
        Instant.ofEpochMilli(r.createdAtMilli)
      )
    }
  }
}
