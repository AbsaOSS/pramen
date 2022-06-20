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

import java.time.LocalDate

/**
  * An aggregation job is a job that can take data from more than one table and more than one
  * information date at the same time.
  *
  * Each aggregation job is fully responsible how input data is read and output (if any)
  * is saved. Pramen invokes the job for tables and input tables that have changed.
  */
trait AggregationJob extends Job {
  /**
    * An aggregation job invokes this method for each of tasks.
    * Each task takes a list of input table names and an information date to run it.
    *
    * If task fails it should throw a runtime exception.
    *
    * @param inputTables A list of input table names that have new data for the info date.
    * @param infoDateBegin    The beginning date of the schedule period.
    * @param infoDateEnd      The ending date of the schedule period.
    * @param infoDateOutput   The information date of the output table.
    * @return A number of records written (if this is available/applicable).
    */
  def runTask(inputTables: Seq[String],
              infoDateBegin: LocalDate,
              infoDateEnd: LocalDate,
              infoDateOutput: LocalDate): Option[Long]
}
