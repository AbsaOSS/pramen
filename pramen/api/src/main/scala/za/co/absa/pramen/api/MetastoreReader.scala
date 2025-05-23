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

import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.offset.DataOffset
import za.co.absa.pramen.api.status.TaskRunReason

import java.time.LocalDate

/**
  * Metastore reader allows querying tables registered at the 'metastore' section of the configuration.
  * It abstracts away the storage provider (HDFS, S3, etc), format (Parquet, Delta, etc.) and partitioning options.
  */
trait MetastoreReader {

  /**
    * Reads a table given the range of information dates, and returns back the dataframe.
    *
    * In order to read a table it is not sufficient the table to be registered in the metastore. It also
    * should be defined as input tables of the job. Otherwise, a runtime exception will be thrown.
    *
    * @param tableName    The name of the table to read.
    * @param infoDateFrom The starting info date to fetch data from (inclusive). Uses the current information date if None.
    * @param infoDateTo   The ending info date (inclusive). Uses the current information date if None.
    * @return The dataframe containing data from the table.
    */
  def getTable(tableName: String,
               infoDateFrom: Option[LocalDate] = None,
               infoDateTo: Option[LocalDate] = None): DataFrame

  /**
    * Reads the 'current batch' of the table to be processed incrementally.
    *
    * For incremental processing this method returns the current chunk being processed.
    * It may include multiple chunks from non-processed data if transformer has failed previously.
    *
    * For non-incremental processing the call to this method is equivalent to:
    * {{{
    *   val df = getTable(tableName)
    * }}}
    *
    * which returns all data for the current information date being processed.
    *
    * This method is the method to use for transformers that would use 'incremental' schedule.
    *
    * In order to read a table it is not sufficient the table to be registered in the metastore. It also
    * should be defined as input tables of the job. Otherwise, a runtime exception will be thrown.
    *
    * @param tableName    The name of the table to read.
    * @return The dataframe containing data from the table.
    */
  def getCurrentBatch(tableName: String): DataFrame

  /**
    * Reads the latest partition of a given table.
    *
    * In order to read a table it is not sufficient the table to be registered in the metastore. It also
    * should be defined as input tables of the job. Otherwise, a runtime exception will be thrown.
    *
    * @param tableName The name of the table to read.
    * @param until     An optional upper boundary. When you run historical transformations you might want to limit the
    *                  recency of input data. Uses the current information date if None.
    * @return The dataframe containing data from the table.
    */
  def getLatest(tableName: String, until: Option[LocalDate] = None): DataFrame

  /**
    * Returns the latest information date the table has data for.
    *
    * In order to read a table it is not sufficient the table to be registered in the metastore. It also
    * should be defined as input tables of the job. Otherwise, a runtime exception will be thrown.
    *
    * @param tableName The name of the table to read.
    * @param until     An optional upper boundary. When you run historical transformations you might want to limit the recency of input data.
    * @return The latest information date the table has data for, None otherwise.
    */
  def getLatestAvailableDate(tableName: String, until: Option[LocalDate] = None): Option[LocalDate]

  /**
    * Returns true if data for the specified table is available for the specified range.
    *
    * This method can be used for validations.
    *
    * @param tableName The name of the table to read.
    * @param from      The starting info date of the availability of the table (inclusive).
    * @param until     An upper boundary. When you run historical transformations you might want to limit the recency of input data.
    * @return true if data is available for the specified range.
    */
  def isDataAvailable(tableName: String, from: Option[LocalDate], until: Option[LocalDate]): Boolean

  /**
    * Returns offsets for an information date (both committed and uncommitted).
    *
    * This info can be used by transformers and sinks to decide if actions need to be taken depending on the
    * current micro batch. For example, adding partitions to Hive needs to happen only once per info date,
    * so a sink that does this can check if micro-batches have been ran for the current day.
    */
  def getOffsets(table: String, infoDate: LocalDate): Array[DataOffset]

  /**
    * Gets definition of a metastore table. Please, use with caution and do not write to the underlying path
    * from transformers.
    *
    * @param tableName The name of the table to query.
    * @return The table definition.
    */
  def getTableDef(tableName: String): MetaTableDef

  /**
    * Returns the run info of a metastore table based on bookkeeping information.
    *
    * It is available only if bookkeeping is used.
    *
    * @param tableName The name of the table in the metastore.
    * @param infoDate  The information date of the data.
    * @return The run info of the table if available.
    */
  def getTableRunInfo(tableName: String, infoDate: LocalDate): Option[MetaTableRunInfo]

  /**
    * Returns the reason of running the task. This helps transformers and sinks to determine logic based on whether
    * thr run is a normal run or a force re-run.
    */
  def getRunReason: TaskRunReason

  /**
    * Returns an object that allows accessing metadata of metastore tables.
    */
  def metadataManager: MetadataManager
}
