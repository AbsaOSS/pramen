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

trait SyncWatcherAggregationJob extends SyncWatcherJob {
  /**
    * An Pramen aggregation job invokes this method for each of tasks.
    * Each task takes a list of input table names and an information date to run it.
    *
    * If task fails it should throw a runtime exception.
    * If an exception is thrown the status of the job state will be considered 'Failed'.
    * The message of the exception will be included in the email notification and stored in the journal.
    *
    * If the requirements to run the job are not met the method should return either 'NotReady' or
    * 'Skipped'.
    * - Reason.NotReady means that some of dependent tables are not up to date. The job can be re-run later then
    *   these dependencies are met.
    * - Reason.Skipped means the job is so late that the data for the output information date cannot possibly
    *   be computed. This can happen for input tables that don't store historical information.
    *   Once the job is skipped it won't be run later.
    *
    * @param taskDependencies The list of tables and latest information dates available for each of them.
    * @param infoDateBegin    The beginning date of the schedule period.
    * @param infoDateEnd      The ending date of the schedule period.
    * @param infoDateOutput   The information date of the output table.
    * @return A number of records written (if the job succeeded, e.g. Right(count))
    *         or the reason of the failure.
    */
  def runTask(taskDependencies: Seq[TaskDependency],
              infoDateBegin: LocalDate,
              infoDateEnd: LocalDate,
              infoDateOutput: LocalDate): Either[Reason, Long]

}
