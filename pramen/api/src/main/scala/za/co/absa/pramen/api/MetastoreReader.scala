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

import java.time.LocalDate

/**
  * Metastore reader allows querying tables registered at the 'metastore' section of the configuration.
  * It abstracts away the storage provider (HDFS, S3, etc), format (Parquet, Delta, etc.) and partitioning options.
  */
trait MetastoreReader {

  /**
    * Reads a table given th range of information dates, and returns back the dataframe.
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
    * Returns an object that allows accessing metadata of metastore tables.
    */
  def metadataManager: MetadataManager
}
