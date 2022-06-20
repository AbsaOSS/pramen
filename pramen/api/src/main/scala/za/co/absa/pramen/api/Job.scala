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

import za.co.absa.pramen.api.schedule.{EveryDay, Schedule}

import java.time.LocalDate

/** This is a base class for all Pramen jobs. */
trait Job {
  /** A name of a job. */
  def name: String

  /** A name of a job. */
  def getSchedule: Schedule = EveryDay()

  /**
    * Depending on the current state of processed tables described in a bookkeeper object
    * return a list of tasks to be done.
    *
    * Each task is a transformation of one or more input tables into one output table for a
    * specific information date.
    *
    * @return A list of tasks that can be done.
    */
  def getDependencies: Seq[JobDependency]

  /**
    * By default output info date is the last day of the scheduling period.
    * A job might want to change this, usually info date might be T-1.
    *
    * @param infoDate An information date.
    * @return A transformed information date.
    */
  def transformOutputInfoDate(infoDate: LocalDate): LocalDate = infoDate

  /**
    * Job can select which info dates to process.
    *
    * Dates that are filtered will be skipped from processing.
    * Dates skipped this way will be considered done and will never be calculated again
    * unless new source data data comes in.
    *
    * @param newDataAvailable     a list of information dates that were updated in at least one input table.
    * @param latestOutputInfoDate The latest information date for which results were already generated.
    * @return A list of info dates to process.
    */
  def selectInfoDates(newDataAvailable: Array[LocalDate], latestOutputInfoDate: Option[LocalDate]): Array[LocalDate] = newDataAvailable

  /**
    * This method is invoked for each selected information date.
    * Pramen provides the list of tables and the latest information date (up to
    * output information date) available for that table.
    *
    * If a task is not ready to run it will not be considered done and dependencies for that
    * information date will be checked again when new data is available.
    *
    * If the task is not ready it should thrown an exception derived from RuntimeException
    * with a message to be added to the notification about the reason of the task is not ready.
    *
    * @param taskDependencies The list of tables and latest information dates available for each of them.
    * @param infoDateBegin    The beginning date of the schedule period.
    * @param infoDateEnd      The ending date of the schedule period.
    * @param infoDateOutput   The information date of the output table.
    * @throws RuntimeException Throws an exception if the task cannot run for some reason.
    */
  def validateTask(taskDependencies: Seq[TaskDependency],
                   infoDateBegin: LocalDate,
                   infoDateEnd: LocalDate,
                   infoDateOutput: LocalDate): Unit = {}

}
