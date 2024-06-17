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

package za.co.absa.pramen.core.runner.splitter

import za.co.absa.pramen.api.status.{MetastoreDependency, TaskRunReason}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.expr.DateExprEvaluator
import za.co.absa.pramen.core.pipeline
import za.co.absa.pramen.core.pipeline.TaskPreDef
import za.co.absa.pramen.core.schedule.Schedule

import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ScheduleStrategyUtils {
  private val log = org.slf4j.LoggerFactory.getLogger(this.getClass)

  val RUN_DATE_VAR1 = "runDate"
  val RUN_DATE_VAR2 = "date"

  val INFO_DATE_VAR = "infoDate"

  /**
    * The user has requested to rerun the pipeline for the specific date. All other checks are skipped.
    *
    * @param outputTable        output table name of the job
    * @param runDate            the date when the job is running on
    * @param infoDateExpression the expression used to calculate info date by the run date
    * @return The sequence of information dates to run. In case of the rerun it will be just one date
    */
  private[core] def getRerun(outputTable: String,
                             runDate: LocalDate,
                             schedule: Schedule,
                             infoDateExpression: String,
                             bookkeeper: Bookkeeper
                            ): List[TaskPreDef] = {
    if (schedule.isEnabled(runDate)) {
      val infoDate = evaluateRunDate(runDate, infoDateExpression)

      bookkeeper.getLatestDataChunk(outputTable, infoDate, infoDate) match {
        case Some(_) =>
          log.info(s"Rerunning '$outputTable' for date $runDate. Info date = '$infoDateExpression' = $infoDate.")
          List(pipeline.TaskPreDef(infoDate, TaskRunReason.Rerun))
        case None =>
          log.info(s"Running '$outputTable' for date $runDate. Info date = '$infoDateExpression' = $infoDate.")
          List(pipeline.TaskPreDef(infoDate, TaskRunReason.New))
      }
    } else {
      log.info(s"The job for '$outputTable' is out of schedule $schedule for $runDate. Skipping...")
      Nil
    }
  }

  /**
    * Returns the information date of the job to run if things go as scheduled.
    *
    * @param outputTable        output table name of the job
    * @param runDate            the date when the job is running on
    * @param schedule           the schedule of the job
    * @param infoDateExpression the expression used to calculate info date by the run date
    * @return Information date of the job to run, if any
    */
  private[core] def getNew(outputTable: String,
                           runDate: LocalDate,
                           schedule: Schedule,
                           infoDateExpression: String
                          ): Option[TaskPreDef] = {
    if (schedule.isEnabled(runDate)) {
      val infoDate = evaluateRunDate(runDate, infoDateExpression)

      log.info(s"For $outputTable $runDate is one of scheduled days. Adding infoDate = '$infoDateExpression' = $infoDate to check.")

      Option(pipeline.TaskPreDef(infoDate, TaskRunReason.New))
    } else {
      log.info(s"For $outputTable $runDate is out of scheduled days. Skipping.")

      None
    }
  }

  private[core] def getLate(outputTable: String,
                            runDate: LocalDate,
                            schedule: Schedule,
                            infoDateExpression: String,
                            initialDateExpr: String,
                            lastProcessedDate: Option[LocalDate]
                           ): List[TaskPreDef] = {
    val lastInfoDate = evaluateRunDate(runDate.minusDays(1), infoDateExpression)
    log.info(s"Closest late info date: $lastInfoDate")

    lastProcessedDate match {
      case Some(lastUpdatedInfoDate) =>
        val nextExpected = getNextExpectedInfoDate(lastUpdatedInfoDate, infoDateExpression, schedule)
        log.info(s"Next expected date: $nextExpected")

        if (nextExpected.toEpochDay <= lastInfoDate.toEpochDay) {
          val range = getInfoDateRange(nextExpected, runDate.minusDays(1), infoDateExpression, schedule)

          log.info(s"Getting possible late run dates in range '$nextExpected'..'${runDate.minusDays(1)}': ${range.mkString(", ")}")

          if (range.nonEmpty) {
            log.info(s"Adding catch up jobs for info dates: ${range.mkString(", ")}")
          }
          range.map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))
        } else {
          log.info(s"No late dates to process")
          Nil
        }
      case None                      =>
        log.info(s"No jobs for $outputTable have ran yet. Getting the initial sourcing date: $initialDateExpr, runDate=$runDate.")
        val initialDate = evaluateRunDate(runDate, initialDateExpr)
        if (initialDate.toEpochDay <= lastInfoDate.toEpochDay) {
          log.info(s"Running from the starting date: $initialDate to the current catch up information date: $lastInfoDate.")
          getInfoDateRange(initialDate, runDate.minusDays(1), infoDateExpression, schedule)
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))
        } else {
          log.info(s"The expression for the initial date returned $initialDate which is after the previous information date $lastInfoDate - no catch up needed.")
          Nil
        }
    }
  }

  private[core] def getHistorical(outputTable: String,
                                  dateFrom: LocalDate,
                                  dateTo: LocalDate,
                                  schedule: Schedule,
                                  mode: RunMode,
                                  infoDateExpression: String,
                                  minimumDate: LocalDate,
                                  inverseDateOrder: Boolean,
                                  bookkeeper: Bookkeeper
                                 ): List[TaskPreDef] = {
    val potentialDates = getInfoDateRange(dateFrom, dateTo, infoDateExpression, schedule)

    val skipAlreadyRanDays = mode == RunMode.SkipAlreadyRan
    val taskReason = if (mode == RunMode.ForceRun)
      TaskRunReason.Rerun
    else
      TaskRunReason.Update

    val datesWithAlreadyRanSkipped = if (skipAlreadyRanDays) {
      potentialDates.filter(date =>
        bookkeeper.getDataChunksCount(outputTable, Some(date), Some(date)) == 0)
        .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))
    } else {
      potentialDates
        .map(d => {
          val chunksCount = bookkeeper.getDataChunksCount(outputTable, Some(d), Some(d))
          val reason = if (chunksCount > 0)
            taskReason
          else TaskRunReason.New
          pipeline.TaskPreDef(d, reason)
        })
    }

    val datesWithProperOrder = if (inverseDateOrder) {
      datesWithAlreadyRanSkipped.reverse
    } else {
      datesWithAlreadyRanSkipped
    }

    filterOutPastMinimumDates(datesWithProperOrder, minimumDate)
  }

  private[core] def filterOutPastMinimumDates(dates: List[TaskPreDef], minimumDate: LocalDate): List[TaskPreDef] = {
    val dayBeforeMinimum = minimumDate.minusDays(1)

    dates.map { taskPreDef =>
      if (taskPreDef.infoDate.isAfter(dayBeforeMinimum)) {
        taskPreDef
      } else {
        taskPreDef.copy(reason = TaskRunReason.Skip(s"The task date '${taskPreDef.infoDate}' is older than the minimum date '$dayBeforeMinimum'."))
      }
    }
  }

  private[core] def anyDependencyUpdatedRetrospectively(outputTable: String,
                                                        infoDate: LocalDate,
                                                        dependencies: Seq[MetastoreDependency],
                                                        bookkeeper: Bookkeeper): Boolean = {
    dependencies.exists(dependency => isDependencyUpdatedRetrospectively(outputTable, infoDate, dependency, bookkeeper))
  }

  private[core] def isDependencyUpdatedRetrospectively(outputTable: String,
                                                       infoDate: LocalDate,
                                                       dependency: MetastoreDependency,
                                                       bookkeeper: Bookkeeper): Boolean = {
    if (!dependency.triggerUpdates) {
      return false
    }

    val lastUpdatedOpt = bookkeeper.getLatestDataChunk(outputTable, infoDate, infoDate)

    val dateFrom = evaluateFromInfoDate(infoDate, dependency.dateFromExpr)
    val dateTo = evaluateFromInfoDate(infoDate, dependency.dateUntilExpr.orNull)

    lastUpdatedOpt match {
      case Some(lastUpdated) =>
        dependency.tables.foldLeft(false)((acc, table) => {
          bookkeeper.getLatestDataChunk(table, dateFrom, dateTo) match {
            case Some(dependencyUpdated) =>
              val isUpdatedRetrospectively = dependencyUpdated.jobFinished > lastUpdated.jobFinished
              if (isUpdatedRetrospectively) {
                log.warn(s"Input table '$table' has updated retrospectively${renderPeriod(Option(dateFrom), Option(dateTo))}. " +
                  s"Adding '$outputTable' to rerun for $infoDate.")
              }
              acc || isUpdatedRetrospectively
            case None                    =>
              acc
          }
        })
      case None              =>
        false
    }
  }

  /**
    * Returns information dates in range from dateFrom to dateTo inclusively.
    *
    * input dates are considering run dates (the dates which job can run)
    * output dates are information dates.
    *
    * @param dateFrom           The beginning of the date range
    * @param dateTo             The end of the date range
    * @param infoDateExpression The expression specifying how to evaluate information date from the run date
    * @param schedule           the schedule of the job
    * @return
    */
  private[core] def getInfoDateRange(dateFrom: LocalDate,
                                     dateTo: LocalDate,
                                     infoDateExpression: String,
                                     schedule: Schedule
                                    ): List[LocalDate] = {
    if (dateFrom.isAfter(dateTo)) {
      Nil
    } else {
      val infoDates = new ListBuffer[LocalDate]()
      val uniqueInfoDates = new mutable.HashSet[LocalDate]()
      var date = dateFrom
      val end = dateTo.plusDays(1)
      while (date.isBefore(end)) {
        if (schedule.isEnabled(date)) {
          val infoDate = evaluateRunDate(date, infoDateExpression)
          if (uniqueInfoDates.add(infoDate)) {
            infoDates += infoDate
          }
        }
        date = date.plusDays(1)
      }
      infoDates.toList
    }
  }

  /**
    * Evaluates the info date expression from the run date.
    *
    * @param runDate       A run date.
    * @param expression    The expression for converting the run date to info date.
    * @param logExpression Whether to log the expression.
    * @return The info date
    */
  def evaluateRunDate(runDate: LocalDate, expression: String, logExpression: Boolean = true): LocalDate = {
    val evaluator = new DateExprEvaluator

    evaluator.setValue(RUN_DATE_VAR1, runDate)
    evaluator.setValue(RUN_DATE_VAR2, runDate)

    val result = evaluator.evalDate(expression)
    if (logExpression) {
      val q = "\""
      log.info(s"Given @runDate = '$runDate', $q$expression$q => infoDate = '$result'")
    }
    result
  }

  /**
    * Evaluates an info date from another info date. This is used to check input table dependencies.
    *
    * @param infoDate   An info Date
    * @param expression A date expression that uses the info date as a variable
    * @return The info date
    */
  def evaluateFromInfoDate(infoDate: LocalDate, expression: String): LocalDate = {
    val evaluator = new DateExprEvaluator

    evaluator.setValue(INFO_DATE_VAR, infoDate)

    evaluator.evalDate(expression)
  }

  /**
    * Renders date range (for logging).
    */
  def renderPeriod(dateFrom: Option[LocalDate], dateTo: Option[LocalDate]): String = {
    (dateFrom, dateTo) match {
      case (Some(from), Some(to)) =>
        s" (from $from to $to)"
      case (Some(from), None)     =>
        s" (from $from)"
      case (None, Some(to))       =>
        s" (up to $to)"
      case (None, None)           =>
        ""
    }
  }


  def getNextExpectedInfoDate(infoDate: LocalDate, infoDateExpression: String, schedule: Schedule): LocalDate = {
    val MAX_ITERATIONS = 100
    var currentInfoDate = infoDate
    var currentRunDate = infoDate
    var iterations = 0
    val fallbackInfoDate = infoDate.plusDays(1)

    while (currentInfoDate.toEpochDay <= infoDate.toEpochDay) {
      iterations += 1
      currentRunDate = currentRunDate.plusDays(1)
      if (schedule.isEnabled(currentRunDate)) {
        val newInfoDate = evaluateRunDate(currentRunDate, infoDateExpression, logExpression = false)

        // If info dates are in the future of run dates for some reason
        if (newInfoDate.isBefore(currentInfoDate)){
          return fallbackInfoDate
        }

        currentInfoDate = newInfoDate
      }

      // If info dates do not change with run dates
      if (iterations > MAX_ITERATIONS) {
        return fallbackInfoDate
      }
    }

    currentInfoDate
  }

  /**
    * Returns the most recent run date that is before the given information date.
    */
  private[core] def getMinRunDateFromInfoDate(infoDate: LocalDate, schedule: Schedule): LocalDate = {
    var currentRunDate = infoDate
    while (!schedule.isEnabled(currentRunDate)) {
      currentRunDate = currentRunDate.minusDays(1)
    }
    currentRunDate
  }

  /**
    * Returns information dates for which the job is enabled in range from infoDateFrom to infoDateTo inclusively.
    */
  def getActiveInfoDates(tableName: String, infoDateFrom: LocalDate, infoDateTo: LocalDate, infoDateExpression: String, schedule: Schedule): Seq[LocalDate] = {
    if (infoDateTo.isBefore(infoDateFrom))
      return Seq.empty

    val testRunDate = evaluateRunDate(infoDateFrom, infoDateExpression, logExpression = false)

    if (testRunDate.isAfter(infoDateFrom)) {
      throw new IllegalArgumentException(s"Could not use forward looking info date expression ($infoDateExpression) for the table '$tableName'.")
    }

    val infoDates = new ListBuffer[LocalDate]
    val minInfoDate = infoDateFrom.minusDays(1)
    val maxInfoDate = infoDateTo.plusDays(1)
    var currentRunDate = getMinRunDateFromInfoDate(infoDateFrom, schedule)
    var currentInfoDate = infoDateFrom
    var lastInfoDate = minInfoDate
    while (currentInfoDate.isBefore(maxInfoDate)) {
      if (schedule.isEnabled(currentRunDate)) {
        currentInfoDate = evaluateRunDate(currentRunDate, infoDateExpression, logExpression = false)
        if (currentInfoDate.isAfter(minInfoDate) && currentInfoDate.isBefore(maxInfoDate) && currentInfoDate != lastInfoDate) {
          infoDates += currentInfoDate
          lastInfoDate = currentInfoDate
        }
      }

      currentRunDate = currentRunDate.plusDays(1)
    }
    log.info(s"For the table '$tableName' period '$infoDateFrom..$infoDateTo' found info dates: ${infoDates.mkString(", ")}")
    infoDates.toSeq
  }

  /**
    * Returns the most recent information date that is before or at the given information date.
    */
  def getLatestActiveInfoDate(tableName: String, infoDateUntil: LocalDate, infoDateExpression: String, schedule: Schedule): LocalDate = {
    val testRunDate = evaluateRunDate(infoDateUntil, infoDateExpression, logExpression = false)

    if (testRunDate.isAfter(infoDateUntil)) {
      throw new IllegalArgumentException(s"Could not use forward looking info date expression ($infoDateExpression) for the table '$tableName'.")
    }

    val maxInfoDate = infoDateUntil.plusDays(1)
    var lastInfoDate = maxInfoDate
    var currentRunDate = getMinRunDateFromInfoDate(infoDateUntil, schedule)
    var currentInfoDate = infoDateUntil
    while (currentInfoDate.isBefore(maxInfoDate)) {
      if (schedule.isEnabled(currentRunDate)) {
        currentInfoDate = evaluateRunDate(currentRunDate, infoDateExpression, logExpression = false)
        if (currentInfoDate.isBefore(maxInfoDate) && currentInfoDate != lastInfoDate) {
          lastInfoDate = currentInfoDate
        }
      }

      currentRunDate = currentRunDate.plusDays(1)
    }
    log.info(s"For the table '$tableName' info date '$infoDateUntil' the most recent snapshot date is '$lastInfoDate'.")
    lastInfoDate
  }
}
