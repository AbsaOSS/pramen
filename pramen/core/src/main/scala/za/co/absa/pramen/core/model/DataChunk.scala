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

import java.time.format.DateTimeFormatter

case class DataChunk(tableName: String,
                     infoDate: String, /* Use String to workaround serialization issues */
                     infoDateBegin: String,
                     infoDateEnd: String,
                     inputRecordCount: Long,
                     outputRecordCount: Long,
                     jobStarted: Long,
                     jobFinished: Long)


object DataChunk {
  /* This is how info dates are stored */
  val datePersistFormat = "yyyy-MM-dd"
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(datePersistFormat)
}
