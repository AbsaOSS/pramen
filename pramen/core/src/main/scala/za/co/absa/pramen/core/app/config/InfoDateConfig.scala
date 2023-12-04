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

package za.co.absa.pramen.core.app.config

import com.typesafe.config.Config
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.DateUtils.convertStrToDate

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class InfoDateConfig(
                           columnName: String,
                           dateFormat: String,
                           startDate: LocalDate,
                           defaultTrackDays: Int,
                           defaultDelayDays: Int,
                           expressionDaily: String,
                           expressionWeekly: String,
                           expressionMonthly: String,
                           initialSourcingDateExprDaily: String,
                           initialSourcingDateExprWeekly: String,
                           initialSourcingDateExprMonthly: String)

object InfoDateConfig {
  val DEFAULT_DATE_FORMAT = "yyyy-MM-dd"

  val INFORMATION_DATE_COLUMN_KEY = "pramen.information.date.column"
  val INFORMATION_DATE_FORMAT_KEY = "pramen.information.date.format"

  val INFORMATION_DATE_START_KEY = "pramen.information.date.start"
  val INFORMATION_DATE_START_DAYS_KEY = "pramen.information.date.max.days.behind"

  val INFORMATION_DATE_EXPRESSION_DAILY_KEY = "pramen.default.daily.output.info.date.expr"
  val INFORMATION_DATE_EXPRESSION_WEEKLY_KEY = "pramen.default.weekly.output.info.date.expr"
  val INFORMATION_DATE_EXPRESSION_MONTHLY_KEY = "pramen.default.monthly.output.info.date.expr"

  val INITIAL_INFORMATION_DATE_EXPRESSION_DAILY_KEY = "pramen.initial.sourcing.date.daily.expr"
  val INITIAL_INFORMATION_DATE_EXPRESSION_WEEKLY_KEY = "pramen.initial.sourcing.date.weekly.expr"
  val INITIAL_INFORMATION_DATE_EXPRESSION_MONTHLY_KEY = "pramen.initial.sourcing.date.monthly.expr"

  val TRACK_DAYS = "pramen.track.days"
  val EXPECTED_DELAY_DAYS = "pramen.expected.delay.days"

  val defaultDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT)
  val defaultStartDate: LocalDate = LocalDate.parse("2010-01-01")

  def fromConfig(conf: Config): InfoDateConfig = {
    val dateFormat = conf.getString(INFORMATION_DATE_FORMAT_KEY)

    val columnName = conf.getString(INFORMATION_DATE_COLUMN_KEY)
    val expressionDaily = conf.getString(INFORMATION_DATE_EXPRESSION_DAILY_KEY)
    val expressionWeekly = conf.getString(INFORMATION_DATE_EXPRESSION_WEEKLY_KEY)
    val expressionMonthly = conf.getString(INFORMATION_DATE_EXPRESSION_MONTHLY_KEY)

    val initialDateExprDaily = conf.getString(INITIAL_INFORMATION_DATE_EXPRESSION_DAILY_KEY)
    val initialDateExprWeekly = conf.getString(INITIAL_INFORMATION_DATE_EXPRESSION_WEEKLY_KEY)
    val initialDateExprMonthly = conf.getString(INITIAL_INFORMATION_DATE_EXPRESSION_MONTHLY_KEY)

    val defaultTrackDays = conf.getInt(TRACK_DAYS)
    val defaultDelayDays = conf.getInt(EXPECTED_DELAY_DAYS)

    val startDateOpt = ConfigUtils.getOptionString(conf, INFORMATION_DATE_START_KEY)
    val startMaxDaysOpt = ConfigUtils.getOptionInt(conf, INFORMATION_DATE_START_DAYS_KEY)

    val startDate = (startDateOpt, startMaxDaysOpt) match {
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException(s"Incompatible options used. Please, use only one of: " +
          s"$INFORMATION_DATE_START_KEY, $INFORMATION_DATE_START_DAYS_KEY")
      case (Some(startDateStr), None) =>
        convertStrToDate(startDateStr, DEFAULT_DATE_FORMAT, dateFormat)
      case (None, Some(days)) =>
        LocalDate.now().minusDays(days)
      case (None, None) =>
        defaultStartDate
    }

    InfoDateConfig(columnName,
      dateFormat,
      startDate,
      defaultTrackDays,
      defaultDelayDays,
      expressionDaily,
      expressionWeekly,
      expressionMonthly,
      initialDateExprDaily,
      initialDateExprWeekly,
      initialDateExprMonthly)
  }
}
