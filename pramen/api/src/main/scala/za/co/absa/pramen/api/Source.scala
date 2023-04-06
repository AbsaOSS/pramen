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
  * A source is an entity from which data is ingested into the metastore. Data at the source is outside of ownership
  * boundary of the pipeline.
  *
  * Ingestion jobs used to read data from a source and write to the metastore.
  * Transfer jobs are used to read data from a source and write it to a sink.
  */
trait Source extends ExternalChannel {
  /**
    * If true, getRecordCount() won't be used to determine if the data is available.
    * This saves performance ond double data read for
    *  - File sources when input files are always available (lookup tables for instance)
    *  - Snapshot-based sources.
    */
  def isDataAlwaysAvailable: Boolean = false

  /**
    * Returns the record count based on the particular input period and query.
    */
  def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long

  /**
    * Returns the data based on the particular input period and query.
    */
  def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult
}
