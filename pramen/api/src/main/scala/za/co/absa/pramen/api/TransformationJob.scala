/*
 * Copyright 2022 ABSA Group Limited
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

trait TransformationJob extends Job {
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
    * @return A table writer, if any. When the writer is None the job is ran for its side effects.
    */
  def getWriter(tableName: String): Option[TableWriter]

  /**
    * A job takes a list of input DataFrames and outputs a dataframe.
    *
    * If task fails it should throw a runtime exception.
    *
    * @param inputTables Input DataFrames. Each DataFrame has a name so that different inputs can be handled differently.
    * @param infoDateBegin  A date of the beginning of information date interval.
    * @param infoDateEnd    A date of the end of information date interval.
    * @param infoDateOutput A date of the information date that is intended to be the output information date (with delay applied).
    * @return The output DataFrame.
    */
  def runTask(inputTables: Seq[TableDataFrame],
              infoDateBegin: LocalDate,
              infoDateEnd: LocalDate,
              infoDateOutput: LocalDate): DataFrame
}
