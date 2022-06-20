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

package za.co.absa.pramen.framework.runner.splitter

import za.co.absa.pramen.api.schedule.Schedule
import za.co.absa.pramen.api.v2.MetastoreDependency
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.job.v2.job.{TaskPreDef, TaskRunReason}
import za.co.absa.pramen.framework.runner.splitter.ScheduleStrategyUtils._

import java.time.LocalDate

class ScheduleStrategyTransformation extends ScheduleStrategy {
  private val log = org.slf4j.LoggerFactory.getLogger(this.getClass)

  override def getDaysToRun(
                             outputTable: String,
                             dependencies: Seq[MetastoreDependency],
                             bookkeeper: SyncBookKeeper,
                             infoDateExpression: String,
                             schedule: Schedule,
                             params: ScheduleParams,
                             minimumDate: LocalDate
                           ): Seq[TaskPreDef] = {
    val dates = params match {
      case ScheduleParams.Normal(runDate, trackDays, delayDays, newOnly, lateOnly)                      =>
        log.info(s"Normal run strategy: runDate=$runDate, trackDays=$trackDays, delayDays=$delayDays, newOnly=$newOnly, lateOnly=$lateOnly")
        val retrospective = getInfoDateRange(runDate.minusDays(trackDays + delayDays), runDate.minusDays(delayDays + 1), infoDateExpression, schedule)
          .filter(date => anyDependencyUpdatedRetrospectively(outputTable, date, dependencies, bookkeeper))
          .map(d => TaskPreDef(d, TaskRunReason.Update))

        val lateDays = if (!newOnly) {
          getLate(outputTable, runDate.minusDays(delayDays), schedule, infoDateExpression, minimumDate, bookkeeper)
        } else {
          Nil
        }

        val newDays = if (!lateOnly) {
          getNew(outputTable, runDate.minusDays(delayDays), schedule, infoDateExpression).toList
        } else {
          Nil
        }

        (retrospective ++ lateDays ++ newDays).groupBy(_.infoDate).map(d => d._2.head).toList.sortBy(a => a.infoDate.toEpochDay)
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
