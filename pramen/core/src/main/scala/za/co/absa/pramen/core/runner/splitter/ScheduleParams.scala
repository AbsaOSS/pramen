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

import za.co.absa.pramen.core.app.config.RuntimeConfig

import java.time.LocalDate

sealed trait ScheduleParams

object ScheduleParams {
  case class Normal(
                     runDate: LocalDate,
                     trackDays: Int,
                     delayDays: Int,
                     newOnly: Boolean,
                     lateOnly: Boolean
                   ) extends ScheduleParams

  case class Rerun(
                    runDate: LocalDate
                  ) extends ScheduleParams

  case class Historical(
                         dateFrom: LocalDate,
                         dateTo: LocalDate,
                         inverseDateOrder: Boolean,
                         mode: RunMode
                       ) extends ScheduleParams

  def fromRuntimeConfig(conf: RuntimeConfig, trackDays: Int, delayDays: Int): ScheduleParams = {
    if (conf.runDateTo.nonEmpty) {
      ScheduleParams.Historical(
        conf.runDate,
        conf.runDateTo.get,
        conf.isInverseOrder,
        conf.historicalRunMode
      )
    } else if (conf.isRerun) {
      ScheduleParams.Rerun(conf.runDate)
    } else {
      ScheduleParams.Normal(conf.runDate,
        trackDays,
        delayDays,
        conf.checkOnlyNewData,
        conf.checkOnlyLateData)
    }
  }
}

