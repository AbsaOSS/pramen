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
import za.co.absa.pramen.core.bookkeeper.model.DataOffsetAggregated
import za.co.absa.pramen.core.pipeline
import za.co.absa.pramen.core.pipeline.TaskPreDef
import za.co.absa.pramen.core.runner.splitter.ScheduleStrategyUtils._
import za.co.absa.pramen.core.schedule.Schedule

import java.time.LocalDate

class ScheduleStrategyIncremental(lastOffsets: Option[DataOffsetAggregated]) extends ScheduleStrategy {
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
      case ScheduleParams.Normal(runDate, _, _, _, _) =>
        val infoDate = evaluateRunDate(runDate, infoDateExpression)
        log.info(s"Normal run strategy: runDate=$runDate, infoDate=$infoDate")

        val runInfoDays = lastOffsets match {
          case Some(offset) =>
            if (offset.maximumInfoDate.isAfter(infoDate)) {
              Seq.empty
            } else {
              Seq(infoDate)
                .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))
            }
          case None =>
            Seq(infoDate)
              .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))
        }

        log.info(s"Days to run:  ${runInfoDays.map(_.infoDate).mkString(", ")}")

        runInfoDays.toList
      case ScheduleParams.Rerun(runDate) =>
        log.info(s"Rerun strategy for a single day: $runDate")
        getRerun(outputTable, runDate, schedule, infoDateExpression, bookkeeper)
      case ScheduleParams.Historical(dateFrom, dateTo, inverseDateOrder, mode) =>
        log.info(s"Ranged strategy: from $dateFrom to $dateTo, mode = '${mode.toString}', minimumDate = $minimumDate")
        getHistorical(outputTable, dateFrom, dateTo, schedule, mode, infoDateExpression, minimumDate, inverseDateOrder, bookkeeper)
    }

    filterOutPastMinimumDates(dates, minimumDate)
  }
}
