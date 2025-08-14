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
import za.co.absa.pramen.api.PartitionScheme
import za.co.absa.pramen.core.utils.ConfigUtils

object PartitionSchemeParser {
  val PARTITION_BY_KEY = "partition.by"
  val PARTITION_PERIOD_KEY = "partition.period"
  val PARTITION_YEAR_COLUMN_KEY = "partition.year.column"
  val PARTITION_MONTH_COLUMN_KEY = "partition.month.column"

  val PARTITION_PERIOD_DAY = "day"
  val PARTITION_PERIOD_MONTH = "month"
  val PARTITION_PERIOD_YEAR_MONTH = "year_month"
  val PARTITION_PERIOD_YEAR = "year"
  val PARTITION_EXPRESSION_OVERWRITE = "overwrite"

  def fromConfig(conf: Config, infoDateColumn: String): Option[PartitionScheme] = {
    val partitionByStrOpt = ConfigUtils.getOptionString(conf, PARTITION_BY_KEY)

    if (partitionByStrOpt.map(_.toLowerCase.trim).contains(PARTITION_EXPRESSION_OVERWRITE)) {
      return Some(PartitionScheme.Overwrite)
    }

    val partitionByOpt = ConfigUtils.getOptionBoolean(conf, PARTITION_BY_KEY)
    val partitionPeriodOpt = ConfigUtils.getOptionString(conf, PARTITION_PERIOD_KEY).map(_.trim.toLowerCase)
    val partitionYearColumn = ConfigUtils.getOptionString(conf, PARTITION_YEAR_COLUMN_KEY).getOrElse(s"${infoDateColumn}_year")
    val partitionMonthColumn = ConfigUtils.getOptionString(conf, PARTITION_MONTH_COLUMN_KEY).getOrElse(s"${infoDateColumn}_month")

    (partitionByOpt, partitionPeriodOpt) match {
      case (_, Some(PARTITION_EXPRESSION_OVERWRITE)) => Some(PartitionScheme.Overwrite)
      case (Some(true), None) => Some(PartitionScheme.PartitionByDay)
      case (Some(false), _) => Some(PartitionScheme.NotPartitioned)
      case (_, Some(PARTITION_PERIOD_DAY)) => Some(PartitionScheme.PartitionByDay)
      case (_, Some(PARTITION_PERIOD_MONTH)) => Some(PartitionScheme.PartitionByMonth(partitionMonthColumn, partitionYearColumn))
      case (_, Some(PARTITION_PERIOD_YEAR_MONTH)) => Some(PartitionScheme.PartitionByYearMonth(partitionMonthColumn))
      case (_, Some(PARTITION_PERIOD_YEAR)) => Some(PartitionScheme.PartitionByYear(partitionYearColumn))
      case (_, Some(period)) if !Seq(PARTITION_PERIOD_DAY, PARTITION_PERIOD_MONTH, PARTITION_PERIOD_YEAR).contains(period) =>
        throw new IllegalArgumentException(s"Invalid value '$period' of '$PARTITION_PERIOD_KEY'. " +
          s"Valid values are: $PARTITION_PERIOD_DAY, $PARTITION_PERIOD_MONTH, $PARTITION_PERIOD_YEAR_MONTH, $PARTITION_PERIOD_YEAR.")
      case _ => None
    }
  }

}
