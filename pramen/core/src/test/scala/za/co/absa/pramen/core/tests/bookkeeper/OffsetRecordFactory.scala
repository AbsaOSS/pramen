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

package za.co.absa.pramen.core.tests.bookkeeper

import za.co.absa.pramen.core.bookkeeper.model.OffsetRecord

object OffsetRecordFactory {
  def getOffsetRecord(pramenTableName: String = "table1",
                      infoDate: String = "2025-10-17",
                      dataType: String = "integral",
                      minOffset: String = "0",
                      maxOffset: String = "10",
                      batchId: Long = 1760607300881L,
                      createdAtMilli: Long = 1760607300881L,
                      committedAtMilli: Option[Long] = Some(1760607359570L)
                     ): OffsetRecord = {
    OffsetRecord(
      pramenTableName = pramenTableName,
      infoDate = infoDate,
      dataType = dataType,
      minOffset = minOffset,
      maxOffset = maxOffset,
      batchId = batchId,
      createdAtMilli = createdAtMilli,
      committedAtMilli = committedAtMilli
    )
  }
}
