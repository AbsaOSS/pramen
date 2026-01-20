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

class ScheduleStrategyIncremental(lastInfoDateProcessedOpt: Option[LocalDate], hasInfoDateColumn: Boolean) extends ScheduleStrategy {
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
      case ScheduleParams.Normal(runDate, backfillDays, trackDays, _, newOnly, lateOnly) =>
        val infoDate = evaluateRunDate(runDate, infoDateExpression)
        log.info(s"Normal run strategy: runDate=$runDate, infoDate=$infoDate")

        val runInfoDays = if (hasInfoDateColumn) {
          lastInfoDateProcessedOpt match {
            case Some(lastInfoDate) =>
              val newDays = if (lastInfoDate.isBefore(infoDate))
                Seq(TaskPreDef(infoDate.minusDays(1), TaskRunReason.New), TaskPreDef(infoDate, TaskRunReason.New))
              else
                Seq(TaskPreDef(infoDate, TaskRunReason.New))

              val lateDays = getLateDays(infoDate, lastInfoDate, trackDays)

              if (newOnly) {
                newDays
              } else if (lateOnly) {
                lateDays
              } else {
                lateDays ++ newDays
              }
            case None => Seq(TaskPreDef(infoDate, TaskRunReason.New))
          }
        } else {
          lastInfoDateProcessedOpt match {
            case Some(lastInfoDate) if lastInfoDate.isAfter(infoDate) => Seq.empty
            case _ => Seq(TaskPreDef(infoDate, TaskRunReason.New))
          }
        }

        if (runInfoDays.nonEmpty) {
          log.info(s"Days to run:  ${runInfoDays.map(_.infoDate).mkString(", ")}")
        } else {
          log.info(s"Days to run: no days have been selected to run by the scheduler.")
        }

        runInfoDays.toList
      case ScheduleParams.Rerun(runDate) =>
        log.info(s"Rerun strategy for a single day: $runDate")
        val infoDate = evaluateRunDate(runDate, infoDateExpression)

        log.info(s"Rerunning '$outputTable' for date $runDate. Info date = '$infoDateExpression' = $infoDate.")
        List(pipeline.TaskPreDef(infoDate, TaskRunReason.Rerun))
      case ScheduleParams.Historical(dateFrom, dateTo, inverseDateOrder, mode) =>
        log.info(s"Ranged strategy: from $dateFrom to $dateTo, mode = '${mode.toString}', minimumDate = $minimumDate")
        getHistorical(outputTable, dateFrom, dateTo, schedule, mode, infoDateExpression, minimumDate, inverseDateOrder, bookkeeper)
    }

    filterOutPastMinimumDates(dates, minimumDate)
  }

  private[core] def getLateDays(infoDate: LocalDate, lastInfoDate: LocalDate, trackDays: Int): Seq[TaskPreDef] = {
    val lastNewDate = infoDate.minusDays(1)

    if (lastInfoDate.isBefore(lastNewDate)) {
      val startDate = if (trackDays > 1) {
        val trackDate = lastNewDate.minusDays(trackDays - 1)
        val date = if (trackDate.isAfter(lastInfoDate))
          trackDate
        else
          lastInfoDate
        log.warn(s"Last ran day: $lastInfoDate. Tracking days = '$trackDate'. Catching up from '$date' until '$infoDate'.")
        date
      } else {
        if (trackDays < 0) {
          log.warn(s"Last ran day: $lastInfoDate. Catching up data until '$infoDate'.")
          lastInfoDate
        } else {
          log.warn(s"Last ran day: $lastInfoDate. Not catching up since 'track.days=$trackDays'. Set it to the number of days or '-1' for infinite catching up.")
          lastNewDate
        }
      }

      val potentialDates = getInfoDateRange(startDate, lastNewDate.minusDays(1), "@runDate", Schedule.Incremental)
      potentialDates.map(date => {
        TaskPreDef(date, TaskRunReason.Late)
      })
    } else {
      Seq.empty
    }
  }
}

