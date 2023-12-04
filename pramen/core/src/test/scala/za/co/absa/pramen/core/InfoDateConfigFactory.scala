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

package za.co.absa.pramen.core

import za.co.absa.pramen.core.app.config.InfoDateConfig

import java.time.LocalDate

object InfoDateConfigFactory {
  def getDummyInfoDateConfig(columnName: String = "pramen_info_date",
                             dateFormat: String = "yyyy-MM-dd",
                             startDate: LocalDate = LocalDate.parse("2010-01-01"),
                             defaultTrackDays: Int = 0,
                             defaultDelayDays: Int = 0,
                             expressionDaily: String = "@runDate",
                             expressionWeekly: String = "lastMonday(@runDate)",
                             expressionMonthly: String = "beginOfMonth(@runDate)",
                             initialSourcingDateExprDaily: String = "@runDate",
                             initialSourcingDateExprWeekly: String = "@runDate - 6",
                             initialSourcingDateExprMonthly: String = "beginOfMonth(@runDate)"): InfoDateConfig = {
    InfoDateConfig(
      columnName,
      dateFormat,
      startDate,
      defaultTrackDays,
      defaultDelayDays,
      expressionDaily,
      expressionWeekly,
      expressionMonthly,
      initialSourcingDateExprDaily,
      initialSourcingDateExprWeekly,
      initialSourcingDateExprMonthly
    )
  }

}
