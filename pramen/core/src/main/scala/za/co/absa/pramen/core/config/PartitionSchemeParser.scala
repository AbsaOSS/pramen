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
  val PARTITION_COLUMN_KEY = "partition.column"

  val PARTITION_PERIOD_DAY = "day"
  val PARTITION_PERIOD_MONTH = "month"
  val PARTITION_PERIOD_YEAR = "year"

  def fromConfig(conf: Config, infoDateColumn: String): Option[PartitionScheme] = {
    val partitionByOpt = ConfigUtils.getOptionBoolean(conf, PARTITION_BY_KEY)
    val partitionPeriodOpt = ConfigUtils.getOptionString(conf, PARTITION_PERIOD_KEY).map(_.trim.toLowerCase)
    val partitionColumnOpt = ConfigUtils.getOptionString(conf, PARTITION_COLUMN_KEY)

    (partitionByOpt, partitionPeriodOpt, partitionColumnOpt) match {
      case (Some(false), _, _) => Some(PartitionScheme.NotPartitioned)
      case (_, Some(PARTITION_PERIOD_DAY), _) => Some(PartitionScheme.PartitionByDay)
      case (_, Some(PARTITION_PERIOD_MONTH), Some(column)) => Some(PartitionScheme.PartitionByMonth(column))
      case (_, Some(PARTITION_PERIOD_MONTH), None) => Some(PartitionScheme.PartitionByMonth(s"${infoDateColumn}_month"))
      case (_, Some(PARTITION_PERIOD_YEAR), Some(column)) => Some(PartitionScheme.PartitionByYear(column))
      case (_, Some(PARTITION_PERIOD_YEAR), None) => Some(PartitionScheme.PartitionByYear(s"${infoDateColumn}_year"))
      case (_, Some(period), _) if !Seq(PARTITION_PERIOD_DAY, PARTITION_PERIOD_MONTH, PARTITION_PERIOD_YEAR).contains(period) =>
        throw new IllegalArgumentException(s"Invalid value '$period' of '$PARTITION_PERIOD_KEY'. " +
          s"Valid values are: $PARTITION_PERIOD_DAY, $PARTITION_PERIOD_MONTH, $PARTITION_PERIOD_YEAR.")
      case (None, None, _) => None
    }
  }

}
