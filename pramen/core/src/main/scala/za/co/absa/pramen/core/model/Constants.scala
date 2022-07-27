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

package za.co.absa.pramen.framework.model

object Constants {
  val BOOKKEEPING_FILE_NAME = "pramen-status.json"

  val JOURNAL_FILE_NAME = "pramen-journal.csv"

  val DATE_FORMAT_INTERNAL = "yyyy-MM-dd"


  // Table writer metadata keys
  val METADATA_LAST_SIZE_WRITTEN = "last_size_written"   // The number of bytes written by the last write operation. Type: Long
}
