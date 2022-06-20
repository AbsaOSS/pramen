/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.pramen.builtin.sink

import com.typesafe.config.Config
import za.co.absa.pramen.framework.AppContextFactory
import za.co.absa.pramen.framework.utils.ConfigUtils

import java.time.ZoneId

case class EnceladusConfig(
                            syncWatcherVersion: String,
                            timezoneId: ZoneId,
                            infoDateColumn: String,
                            partitionPattern: String,
                            format: String,
                            formatOptions: Map[String, String],
                            mode: String,
                            recordsPerPartition: Option[Int],
                            saveEmpty: Boolean,
                            generateInfoFile: Boolean
                          )

object EnceladusConfig {
  val INFO_DATE_COLUMN_KEY = "info.date.column"
  val PARTITION_PATTERN_KEY = "partition.pattern"
  val FORMAT_KEY = "format"
  val MODE_KEY = "mode"
  val RECORDS_PER_PARTITION = "records.per.partition"
  val SAVE_EMPTY_KEY = "save.empty"
  val GENERATE_INFO_FILE_KEY = "info.file.generate"

  val DEFAULT_INFO_DATE_COLUMN = "enceladus_info_date"
  val DEFAULT_PARTITION_PATTERN = "{year}/{month}/{day}/v{version}"
  val DEFAULT_FORMAT = "parquet"
  val DEFAULT_MODE = "errorifexists"

  def fromConfig(conf: Config): EnceladusConfig = {
    val appContext = AppContextFactory.get

    val syncWatcherVersion = if (appContext != null) {
      appContext.appConfig.generalConfig.applicationVersion
    } else {
      "Unspecified"
    }

    val timezoneId = if (appContext != null) {
      appContext.appConfig.generalConfig.timezoneId
    } else {
      ZoneId.systemDefault()
    }

    EnceladusConfig(
      syncWatcherVersion,
      timezoneId,
      ConfigUtils.getOptionString(conf, INFO_DATE_COLUMN_KEY).getOrElse(DEFAULT_INFO_DATE_COLUMN),
      ConfigUtils.getOptionString(conf, PARTITION_PATTERN_KEY).getOrElse(DEFAULT_PARTITION_PATTERN),
      ConfigUtils.getOptionString(conf, FORMAT_KEY).getOrElse(DEFAULT_FORMAT),
      ConfigUtils.getExtraOptions(conf, "option"),
      ConfigUtils.getOptionString(conf, MODE_KEY).getOrElse(DEFAULT_MODE),
      ConfigUtils.getOptionInt(conf, RECORDS_PER_PARTITION),
      ConfigUtils.getOptionBoolean(conf, SAVE_EMPTY_KEY).getOrElse(true),
      ConfigUtils.getOptionBoolean(conf, GENERATE_INFO_FILE_KEY).getOrElse(true)
    )
  }
}
