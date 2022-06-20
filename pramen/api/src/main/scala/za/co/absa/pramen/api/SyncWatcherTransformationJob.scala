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

import java.time.LocalDate

import org.apache.spark.sql.DataFrame

trait SyncWatcherTransformationJob extends SyncWatcherJob {

  /**
    * A job takes a list of input DataFrames and outputs a dataframe.
    *
    * If task fails it should throw a runtime exception.
    *
    * If the requirements to run the job are not met the method should return either 'NotReady' or
    * 'Skipped'.
    * - Reason.NotReady means that some of dependent tables are not up to date. The job can be re-run later then
    *   these dependencies are met.
    * - Reason.Skipped means the job is so late that the data for the output information date cannot possibly
    *   be computed. This can happen for input tables that don't store historical information.
    *   Once the job is skipped it won't be run later.
    *
    * @param inputTables Input DataFrames. Each DataFrame has a name so that different inputs can be handled differently.
    * @param infoDateBegin  A date of the beginning of information date interval.
    * @param infoDateEnd    A date of the end of information date interval.
    * @param infoDateOutput A date of the information date that is intended to be the output information date (with delay applied).
    * @return The output DataFrame (if the job succeeded, e.g. Right(df))
    *         or the reason of the failure.
    */
  def runTask(inputTables: Seq[TableDataFrame],
              infoDateBegin: LocalDate,
              infoDateEnd: LocalDate,
              infoDateOutput: LocalDate): Either[Reason, DataFrame]
}
