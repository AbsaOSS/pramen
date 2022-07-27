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

package za.co.absa.pramen.core.config

import com.typesafe.config.Config

/** Default expressions used to calculate information date from run date. */
case class InfoDateExpressions(
                                defaultDailyExpr: String,
                                defaultWeeklyExpr: String,
                                defaultMonthlyExpr: String
                              )

object InfoDateExpressions {
  val DEFAULT_INFO_DATE_DAILY_EXPR_KEY = "pramen.default.daily.output.info.date.expr"
  val DEFAULT_INFO_DATE_WEEKLY_EXPR_KEY = "pramen.default.weekly.output.info.date.expr"
  val DEFAULT_INFO_DATE_MONTHLY_EXPR_KEY = "pramen.default.monthly.output.info.date.expr"

  def fromConfig(config: Config): InfoDateExpressions = {
    val defaultDailyExpr = config.getString(DEFAULT_INFO_DATE_DAILY_EXPR_KEY)
    val defaultWeeklyExpr = config.getString(DEFAULT_INFO_DATE_WEEKLY_EXPR_KEY)
    val defaultMonthlyExpr = config.getString(DEFAULT_INFO_DATE_MONTHLY_EXPR_KEY)
    InfoDateExpressions(defaultDailyExpr, defaultWeeklyExpr, defaultMonthlyExpr)
  }
}