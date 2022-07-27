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

import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.model.MetastoreDependency
import za.co.absa.pramen.core.pipeline
import za.co.absa.pramen.core.pipeline.{TaskPreDef, TaskRunReason}
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
        log.info(s"Normal run strategy: runDate=$runDate, trackDays=$trackDays, delayDays=$delayDays, newOnly=$newOnly, lateOnly=$lateOnly")
        val trackedDays = if (!lateOnly && !newOnly) {
          getInfoDateRange(runDate.minusDays(delayDays + trackDays), runDate.minusDays(delayDays + 1), infoDateExpression, schedule)
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))
        } else {
          Nil
        }

        val lateDays = if (!newOnly) {
          getLate(outputTable, runDate.minusDays(delayDays), schedule, infoDateExpression, initialSourcingDateExpr, bookkeeper)
        } else {
          Nil
        }

        val newDays = if (!lateOnly) {
          getNew(outputTable, runDate.minusDays(delayDays), schedule, infoDateExpression).toList
        } else {
          Nil
        }

        (trackedDays ++ lateDays ++ newDays).groupBy(_.infoDate).map(d => d._2.head).toList.sortBy(a => a.infoDate.toEpochDay)
      case ScheduleParams.Rerun(runDate)                                                     =>
        log.info(s"Rerun strategy for a single day: $runDate")
        getRerun(outputTable, runDate, infoDateExpression)
      case ScheduleParams.Historical(dateFrom, dateTo, inverseDateOrder, mode) =>
        log.info(s"Ranged strategy: from $dateFrom to $dateTo, mode = '${mode.toString}', minimumDate = $minimumDate")
        getHistorical(outputTable, dateFrom, dateTo, schedule, mode, infoDateExpression, minimumDate, inverseDateOrder, bookkeeper)
    }

    val dayBeforeMinimum = minimumDate.minusDays(1)

    dates.filter(_.infoDate.isAfter(dayBeforeMinimum))
  }
}
