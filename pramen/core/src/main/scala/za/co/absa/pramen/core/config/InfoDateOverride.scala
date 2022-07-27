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
import za.co.absa.pramen.core.model.Constants.DATE_FORMAT_INTERNAL
import za.co.absa.pramen.core.utils.ConfigUtils

import java.time.LocalDate

case class InfoDateOverride(
                             columnName: Option[String],
                             dateFormat: Option[String],
                             expression: Option[String],
                             startDate: Option[LocalDate]
                           )

object InfoDateOverride {
  val INFORMATION_DATE_COLUMN_KEY = "information.date.column"
  val INFORMATION_DATE_FORMAT_KEY = "information.date.format"
  val INFORMATION_DATE_EXPRESSION_KEY = "information.date.expression"
  val INFORMATION_DATE_START_KEY = "information.date.start"

  def fromConfig(conf: Config): InfoDateOverride = {
    val columnNameOpt = ConfigUtils.getOptionString(conf, INFORMATION_DATE_COLUMN_KEY)
    val dateFormatOpt = ConfigUtils.getOptionString(conf, INFORMATION_DATE_FORMAT_KEY)
    val expressionOpt = ConfigUtils.getOptionString(conf, INFORMATION_DATE_EXPRESSION_KEY)
    val startDateOpt = ConfigUtils.getDateOpt(conf, INFORMATION_DATE_START_KEY, DATE_FORMAT_INTERNAL)

    InfoDateOverride(columnNameOpt, dateFormatOpt, expressionOpt, startDateOpt)
  }
}
