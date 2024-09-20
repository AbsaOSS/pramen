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

import za.co.absa.pramen.api.offset.{OffsetInfo, OffsetValue}

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
    * If true, the source + query is configured for ingesting events - tables have information date column, and data is filtered
    * by that date when ingested.
    *
    * If false, the source + query is configured for snapshots - tables are loaded fully each day according to the schedule.
    */
  def hasInfoDateColumn(query: Query): Boolean = true

  /**
    * If non-empty, the source is configured for incremental ingestion, returns minimum value with type
    *
    * If empty, the source can't be used for incremental ingestion.
    */
  def getOffsetInfo: Option[OffsetInfo] = None

  /**
    * Validates if the source is okay and the ingestion can proceed.
    */
  def validate(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Reason = Reason.Ready

  /**
    * Returns the record count based on the particular input period and query.
    */
  def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long

  /**
    * Returns the data based on the particular input period and query.
    */
  def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult

  /**
    * Returns the incremental data between specified offsets. The offset intervals could be half open,
    * e.g. only offsetFrom or offsetFrom is specified.
    *
    * If an information date is provided and available at the source, the query will be limited to that date.
    *
    * @param offsetFromOpt   This is an exclusive parameter the query will be SELECT ... WHERE offset_col > min_offset
    * @param offsetToOpt     This is an exclusive parameter the query will be SELECT ... WHERE offset_col > min_offset
    * @param onlyForInfoDate An information date to get data for. Can be empty if the source table doesn't have such a column.
    * @param columns         Select only specified columns. Selects all if an empty Seq is passed.
    */
  def getIncrementalData(query: Query, onlyForInfoDate: Option[LocalDate], offsetFromOpt: Option[OffsetValue], offsetToOpt: Option[OffsetValue], columns: Seq[String]): SourceResult

  /**
    * This method is called after the ingestion is finished. You can query the output table form the output information
    * data and the data should be there.
    *
    * @param query           The query used to read the data from the source.
    * @param outputTableName The table name used as the output table of the ingestion.
    * @param metastore       The read only version of metastore. You can only query tables using it.
    * @param infoDate        The information date of the output of the ingestion.
    * @param options         Extra options specified in the operation definition for the ingestion.
    */
  def postProcess(query: Query,
                  outputTableName: String,
                  metastore: MetastoreReader,
                  infoDate: LocalDate,
                  options: Map[String, String]): Unit = {}
}
