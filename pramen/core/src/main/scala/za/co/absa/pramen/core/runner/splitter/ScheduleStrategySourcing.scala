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

import za.co.absa.pramen.api.jobdef.Schedule
import za.co.absa.pramen.api.status.{MetastoreDependency, TaskRunReason}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.pipeline
import za.co.absa.pramen.core.pipeline.TaskPreDef
import za.co.absa.pramen.core.runner.splitter.ScheduleStrategyUtils._

import java.time.LocalDate

/**
  * The scheduling strategy for Pramen jobs.
  *
  * Given scheduling and runtime parameters, this strategy returns a list of potential information dates to run.
  * These dates don't guarantee job execution. Jobs may be skipped for specific information dates if
  * dependencies are not met or validation fails.
  *
  * @param hasInfoDateColumn If true, the output table has an info date column and is treated as a table containing immutable events.
  */
class ScheduleStrategySourcing(hasInfoDateColumn: Boolean) extends ScheduleStrategy {
  private val log = org.slf4j.LoggerFactory.getLogger(this.getClass)

  override def getDaysToRun(
                             outputTable: String,
                             dependencies: Seq[MetastoreDependency],
                             bookkeeper: Bookkeeper,
                             infoDateExpression: String,
                             schedule: Schedule,
                             params: ScheduleParams,
                             initialSourcingDateExpr: String,
                             minimumDate: LocalDate
                           ): Seq[TaskPreDef] = {
    val dates = params match {
      case ScheduleParams.Normal(runDate, backfillDays, trackDays, delayDays, newOnly, lateOnly)                      =>
        val infoDate = evaluateRunDate(runDate, infoDateExpression)
        log.info(s"Normal run strategy: runDate=$runDate, trackDays=$trackDays, delayDays=$delayDays, newOnly=$newOnly, lateOnly=$lateOnly, infoDate=$infoDate")
        val trackedDays = if (!lateOnly && !newOnly) {
          getInfoDateRange(runDate.minusDays(delayDays + trackDays - 1), runDate.minusDays(delayDays + 1), infoDateExpression, schedule)
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))
        } else {
          Nil
        }

        val lastProcessedDate = bookkeeper.getLatestProcessedDate(outputTable, Option(infoDate))
        lastProcessedDate.foreach(d => log.info(s"Last processed info date: $d"))

        val backfillDates = getBackFillDays(outputTable, runDate, backfillDays, trackDays, lastProcessedDate, schedule, infoDateExpression, bookkeeper)
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))

        val newDaysOrig = if (!lateOnly) {
          getNew(outputTable, runDate.minusDays(delayDays), schedule, infoDateExpression).toList
        } else {
          Nil
        }

        val newDays = lastProcessedDate match {
          case Some(date) if trackDays <= 0 => newDaysOrig.filter(task => task.infoDate.isAfter(date))
          case _                            => newDaysOrig
        }

        val lateDaysOrig = if (!newOnly) {
          getLate(outputTable, runDate.minusDays(delayDays), schedule, infoDateExpression, initialSourcingDateExpr, lastProcessedDate)
        } else {
          Nil
        }

        val lateDays = if (hasInfoDateColumn) {
          lateDaysOrig
        } else {
          if (newDays.isEmpty) {
            lateDaysOrig.lastOption.toSeq
          } else {
            Nil
          }
        }

        log.info(s"Backfill days: ${backfillDates.map(_.infoDate).mkString(", ")}")
        log.info(s"Tracked days: ${trackedDays.map(_.infoDate).mkString(", ")}")
        log.info(s"Late days: ${lateDays.map(_.infoDate).mkString(", ")}")
        log.info(s"New days: ${newDaysOrig.map(_.infoDate).mkString(", ")}")
        log.info(s"New days not ran already: ${newDays.map(_.infoDate).mkString(", ")}")

        (backfillDates ++ trackedDays ++ lateDays ++ newDays).groupBy(_.infoDate).map(d => d._2.head).toList.sortBy(a => a.infoDate.toEpochDay)
      case ScheduleParams.Rerun(runDate)                                                     =>
        log.info(s"Rerun strategy for a single day: $runDate")
        getRerun(outputTable, runDate, schedule, infoDateExpression, bookkeeper)
      case ScheduleParams.Historical(dateFrom, dateTo, inverseDateOrder, mode) =>
        log.info(s"Ranged strategy: from $dateFrom to $dateTo, mode = '${mode.toString}', minimumDate = $minimumDate")
        getHistorical(outputTable, dateFrom, dateTo, schedule, mode, infoDateExpression, minimumDate, inverseDateOrder, bookkeeper)
    }

    filterOutPastMinimumDates(dates, minimumDate)
  }

  def getBackFillDays(outputTable: String,
                      runDate: LocalDate,
                      backfillDays: Int,
                      trackDays: Int,
                      lastProcessedDate: Option[LocalDate],
                      schedule: Schedule,
                      initialSourcingDateExpr: String,
                      bookkeeper: Bookkeeper): Seq[LocalDate] = {
    // If backfillDays == 0, backfill is disabled
    // If trackDays > backfillDays, track days supersede backfill with checks for retrospective updates
    if (backfillDays == 0 || (backfillDays > 0 && trackDays > backfillDays)) return Seq.empty

    val backfillStart = if (backfillDays < 0) {
      lastProcessedDate.getOrElse(runDate)
    } else {
      runDate.minusDays(backfillDays - 1)
    }

    if (backfillStart.isEqual(runDate)) return Seq.empty

    val trackDaysBehind = if (trackDays > 0) trackDays - 1 else 0
    val backfillEnd = runDate.minusDays(trackDaysBehind) // the end backfill date is exclusive

    if (backfillEnd.isBefore(backfillStart) || backfillEnd.isEqual(backfillStart)) return Seq.empty

    val potentialDates = getInfoDateRange(backfillStart, backfillEnd.minusDays(1), initialSourcingDateExpr, schedule)
    if (potentialDates.nonEmpty) {
      val dataAvailability = bookkeeper.getDataAvailability(outputTable, backfillStart, backfillEnd.minusDays(1))
        .map(d => (d.infoDate, d.chunks)).toMap
      potentialDates.filterNot(d => dataAvailability.contains(d))
    } else {
      potentialDates
    }
  }
}
