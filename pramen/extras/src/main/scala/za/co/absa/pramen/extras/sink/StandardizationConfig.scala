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

package za.co.absa.pramen.extras.sink

import com.typesafe.config.Config
import za.co.absa.pramen.api.Pramen
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.hive.HiveFormat

import java.time.ZoneId
import scala.util.Try

case class StandardizationConfig(
                                  pramenVersion: String,
                                  timezoneId: ZoneId,
                                  rawFormat: String,
                                  rawPartitionPattern: String,
                                  publishPartitionPattern: String,
                                  recordsPerPartition: Option[Int],
                                  generateInfoFile: Boolean,
                                  publishFormat: HiveFormat,
                                  hiveJdbcConfig: Option[JdbcConfig],
                                  hiveDatabase: Option[String],
                                  hiveIgnoreFailures: Boolean,
                                  hiveOptimizeExistQuery: Boolean,
                                  infoDateColumn: String,
                                  infoDateStringColumn: String,
                                  infoVersionColumn: String
                                )

object StandardizationConfig {
  val RAW_FORMAT_KEY = "raw.format"
  val RAW_FORMAT_DEFAULT = "parquet"
  val RAW_PARTITION_PATTERN_KEY = "raw.partition.pattern"
  val PUBLISH_PARTITION_PATTERN_KEY = "publish.partition.pattern"
  val PUBLISH_FORMAT_KEY = "publish.format"
  val RECORDS_PER_PARTITION = "records.per.partition"
  val GENERATE_INFO_FILE_KEY = "info.file.generate"
  val TIMEZONE_ID_KEY = "timezone"

  val HIVE_JDBC_PREFIX = "hive"
  val HIVE_DATABASE_KEY = "hive.database"
  val HIVE_IGNORE_FAILURES_KEY = "hive.ignore.failures"
  val HIVE_OPTIMIZE_EXIST_QUERY_KEY = "hive.optimize.exist.query"

  val INFO_DATE_COLUMN_KEY = "info.date.column"
  val INFO_DATE_STRING_COLUMN_KEY = "info.date.str.column"
  val INFO_VERSION_COLUMN_KEY = "info.version.column"

  val INFO_DATE_COLUMN_DEFAULT = "enceladus_info_date"
  val INFO_DATE_STRING_COLUMN_DEFAULT = "enceladus_info_date_string"
  val INFO_VERSION_COLUMN_DEFAULT = "enceladus_info_version"

  val DEFAULT_RAW_PARTITION_PATTERN = "{year}/{month}/{day}/v{version}"
  val DEFAULT_PUBLISH_PARTITION_PATTERN = s"$INFO_DATE_COLUMN_DEFAULT={year}-{month}-{day}/$INFO_VERSION_COLUMN_DEFAULT={version}"

  def fromConfig(conf: Config): StandardizationConfig = {
    val pramenVersion = Try {
      Pramen.instance.buildProperties.getFullVersion
    }.recover { case _ => "unknown" }.get

    val timezoneId = ConfigUtils.getOptionString(conf, TIMEZONE_ID_KEY) match {
      case Some(configuredTimeZoneStr) => ZoneId.of(configuredTimeZoneStr)
      case None                        => ZoneId.systemDefault()
    }

    val jdbcConfig = ConfigUtils.getOptionConfig(conf, HIVE_JDBC_PREFIX)

    val hiveJdbcConfig = JdbcConfig.loadOption(jdbcConfig, HIVE_JDBC_PREFIX)

    val publishFormat = HiveFormat.fromString(ConfigUtils.getOptionString(conf, PUBLISH_FORMAT_KEY).getOrElse("parquet"))

    val infoDateColumn = ConfigUtils.getOptionString(conf, INFO_DATE_COLUMN_KEY).getOrElse(INFO_DATE_COLUMN_DEFAULT)
    val infoDateStringColumn = ConfigUtils.getOptionString(conf, INFO_DATE_STRING_COLUMN_KEY).getOrElse(INFO_DATE_STRING_COLUMN_DEFAULT)
    val infoVersionColumn = ConfigUtils.getOptionString(conf, INFO_VERSION_COLUMN_KEY).getOrElse(INFO_VERSION_COLUMN_DEFAULT)

    StandardizationConfig(
      pramenVersion,
      timezoneId,
      ConfigUtils.getOptionString(conf, RAW_FORMAT_KEY).getOrElse(RAW_FORMAT_DEFAULT),
      ConfigUtils.getOptionString(conf, RAW_PARTITION_PATTERN_KEY).getOrElse(DEFAULT_RAW_PARTITION_PATTERN),
      ConfigUtils.getOptionString(conf, PUBLISH_PARTITION_PATTERN_KEY).getOrElse(DEFAULT_PUBLISH_PARTITION_PATTERN),
      ConfigUtils.getOptionInt(conf, RECORDS_PER_PARTITION),
      ConfigUtils.getOptionBoolean(conf, GENERATE_INFO_FILE_KEY).getOrElse(true),
      publishFormat,
      hiveJdbcConfig,
      ConfigUtils.getOptionString(conf, HIVE_DATABASE_KEY),
      ConfigUtils.getOptionBoolean(conf, HIVE_IGNORE_FAILURES_KEY).getOrElse(false),
      hiveOptimizeExistQuery = ConfigUtils.getOptionBoolean(conf, HIVE_OPTIMIZE_EXIST_QUERY_KEY).getOrElse(true),
      infoDateColumn,
      infoDateStringColumn,
      infoVersionColumn
    )
  }
}
