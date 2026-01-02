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

package za.co.absa.pramen.core.model

import za.co.absa.pramen.core.bookkeeper.model.BookkeepingRecord

import java.time.format.DateTimeFormatter

/**
  * Represents a data chunk for storing bookkeeping records in non-relational storage (e.g., CSV files).
  * The order of columns must be preserved to maintain compatibility with existing CSV files
  * since the field order in CSV directly depends on it.
  */
case class DataChunk(tableName: String,
                     infoDate: String,
                     infoDateBegin: String,
                     infoDateEnd: String,
                     inputRecordCount: Long,
                     outputRecordCount: Long,
                     jobStarted: Long,
                     jobFinished: Long,
                     batchId: Option[Long],
                     appendedRecordCount: Option[Long])


object DataChunk {
  /* This is how info dates are stored */
  val datePersistFormat = "yyyy-MM-dd"
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(datePersistFormat)

  def fromRecord(r: BookkeepingRecord): DataChunk = {
    DataChunk(r.pramenTableName, r.infoDate, r.infoDateBegin, r.infoDateEnd, r.inputRecordCount, r.outputRecordCount, r.jobStarted, r.jobFinished, r.batchId, r.appendedRecordCount)
  }
}
