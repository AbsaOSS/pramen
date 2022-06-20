/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.writer.TableWriter

import java.time.LocalDate

/**
  * This type of job is for synchronizing a source tables with a store under Pramen control.
  * Reader is usually a JDBC connection while Writer is usually Parquet.
  */
trait SourceJob extends Job {
  def getTables: Seq[String]

  /**
    * Source jobs process tables 1 to 1. Each source table corresponds to a sink table
    */
  final override def getDependencies: Seq[JobDependency] = getTables.map(tableName => JobDependency(Seq(tableName), tableName))

  /**
    * Returns a reader given a table name. The reader should be able to get data from the table based on
    * information date.
    *
    * @param tableName A table name
    * @return A table reader.
    */
  def getReader(tableName: String): TableReader

  /**
    * Returns a writer given a table name. The writer should be able to overwrite a portion of the table
    * given an information date.
    *
    * @param tableName A table name
    * @return A table writer.
    */
  def getWriter(tableName: String): TableWriter

  /**
    * A job takes a DataFrame and returns a DataFrame.
    *
    * By default the job just copies data from one location to another.
    *
    * @param inputTable     An input DataFrame to synchronize.
    * @param infoDateBegin  A date of the beginning of information date interval.
    * @param infoDateEnd    A date of the end of information date interval.
    * @param infoDateOutput A date of the information date that is intended to be the output information date (with delay applied).
    * @return The output DataFrame.
    */
  def runTask(inputTable: TableDataFrame,
              infoDateBegin: LocalDate,
              infoDateEnd: LocalDate,
              infoDateOutput: LocalDate): DataFrame = inputTable.dataFrame
}
