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
import za.co.absa.pramen.core.pipeline
import za.co.absa.pramen.core.pipeline.TaskPreDef
import za.co.absa.pramen.core.runner.splitter.ScheduleStrategyUtils._
import za.co.absa.pramen.core.schedule.Schedule

import java.time.LocalDate

class ScheduleStrategySourcing extends ScheduleStrategy {
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
      case ScheduleParams.Normal(runDate, trackDays, delayDays, newOnly, lateOnly)                      =>
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

        val lateDays = if (!newOnly) {
          getLate(outputTable, runDate.minusDays(delayDays), schedule, infoDateExpression, initialSourcingDateExpr, lastProcessedDate)
        } else {
          Nil
        }

        val newDaysOrig = if (!lateOnly) {
          getNew(outputTable, runDate.minusDays(delayDays), schedule, infoDateExpression).toList
        } else {
          Nil
        }

        val newDays = lastProcessedDate match {
          case Some(date) if trackDays <= 0 => newDaysOrig.filter(task => task.infoDate.isAfter(date))
          case _                            => newDaysOrig
        }

        log.info(s"Tracked days: ${trackedDays.map(_.infoDate).mkString(", ")}")
        log.info(s"Late days: ${lateDays.map(_.infoDate).mkString(", ")}")
        log.info(s"New days: ${newDaysOrig.map(_.infoDate).mkString(", ")}")
        log.info(s"New days not ran already: ${newDays.map(_.infoDate).mkString(", ")}")

        (trackedDays ++ lateDays ++ newDays).groupBy(_.infoDate).map(d => d._2.head).toList.sortBy(a => a.infoDate.toEpochDay)
      case ScheduleParams.Rerun(runDate)                                                     =>
        log.info(s"Rerun strategy for a single day: $runDate")
        getRerun(outputTable, runDate, schedule, infoDateExpression, bookkeeper)
      case ScheduleParams.Historical(dateFrom, dateTo, inverseDateOrder, mode) =>
        log.info(s"Ranged strategy: from $dateFrom to $dateTo, mode = '${mode.toString}', minimumDate = $minimumDate")
        getHistorical(outputTable, dateFrom, dateTo, schedule, mode, infoDateExpression, minimumDate, inverseDateOrder, bookkeeper)
    }

    filterOutPastMinimumDates(dates, minimumDate)
  }
}
