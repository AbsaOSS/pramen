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

package za.co.absa.pramen.api

import java.time.LocalDate

/**
  * A file source is a source used for ingesting files most of the time without looking into their contents.
  *
  * The idea is so sort files in a drop area into raw area for further processing.
  */
trait FileSource extends ExternalChannel {
  /**
    * Validates if files on the source are present and okay.
    */
  def validate(query: Query, infoDate: LocalDate): Reason = Reason.Ready

  /**
    * Returns the number of files available for the specified information date.
    */
  def getFileCount(query: Query, infoDate: LocalDate): Long

  /**
    * Returns the lift of files to be loaded from the source location.
    */
  def getFileList(query: Query, infoDate: LocalDate): FileSourceResult
}
