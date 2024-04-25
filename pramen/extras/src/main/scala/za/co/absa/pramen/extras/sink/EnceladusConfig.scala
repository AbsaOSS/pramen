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
import za.co.absa.pramen.extras.utils.ConfigUtils

import java.time.ZoneId
import scala.util.Try

case class EnceladusConfig(
                            pramenVersion: String,
                            timezoneId: ZoneId,
                            infoDateColumn: String,
                            partitionPattern: String,
                            format: String,
                            formatOptions: Map[String, String],
                            mode: String,
                            recordsPerPartition: Option[Int],
                            saveEmpty: Boolean,
                            generateInfoFile: Boolean,
                            preferAddPartition: Boolean,
                            enceladusMainClass: String,
                            enceladusCmdLineTemplate: String,
                            hiveDatabase: Option[String],
                            publishPartitionPattern: String
                          )

object EnceladusConfig {
  val INFO_DATE_COLUMN_KEY = "info.date.column"
  val PARTITION_PATTERN_KEY = "partition.pattern"
  val FORMAT_KEY = "format"
  val MODE_KEY = "mode"
  val RECORDS_PER_PARTITION = "records.per.partition"
  val SAVE_EMPTY_KEY = "save.empty"
  val GENERATE_INFO_FILE_KEY = "info.file.generate"
  val PREFER_ADD_PARTITION_KEY = "prefer.add.partition"
  val TIMEZONE_ID_KEY = "timezone"

  val ENCELADUS_RUN_MAIN_CLASS_KEY = "enceladus.run.main.class"
  val ENCELADUS_COMMAND_LINE_TEMPLATE_KEY = "enceladus.command.line.template"
  val HIVE_DATABASE_KEY = "hive.database"
  val PUBLISH_PARTITION_TEMPLATE_KEY = "publish.partition.template"

  val DEFAULT_INFO_DATE_COLUMN = "enceladus_info_date"
  val DEFAULT_PARTITION_PATTERN = "{year}/{month}/{day}/v{version}"
  val DEFAULT_FORMAT = "parquet"
  val DEFAULT_MODE = "errorifexists"
  val DEFAULT_ENCELADUS_RUN_MAIN_CLASS = "za.co.absa.enceladus.standardization_conformance.StandardizationAndConformanceJob"
  val DEFAULT_ENCELADUS_COMMAND_LINE_TEMPLATE = "--dataset-name @datasetName --dataset-version @datasetVersion --report-date @infoDate --report-version @infoVersion --menas-auth-keytab menas.keytab --raw-format @rawFormat --autoclean-std-folder true"
  val DEFAULT_PUBLISH_PARTITION_TEMPLATE = "enceladus_info_date={year}-{month}-{day}/enceladus_info_version={version}"

  def fromConfig(conf: Config): EnceladusConfig = {
    val pramenVersion = Try {
      Pramen.instance.buildProperties.getFullVersion
    }.recover{case _ => "unknown"}.get

    val timezoneId = ConfigUtils.getOptionString(conf, TIMEZONE_ID_KEY) match {
      case Some(configuredTimeZoneStr) => ZoneId.of(configuredTimeZoneStr)
      case None => ZoneId.systemDefault()
    }

    EnceladusConfig(
      pramenVersion,
      timezoneId,
      ConfigUtils.getOptionString(conf, INFO_DATE_COLUMN_KEY).getOrElse(DEFAULT_INFO_DATE_COLUMN),
      ConfigUtils.getOptionString(conf, PARTITION_PATTERN_KEY).getOrElse(DEFAULT_PARTITION_PATTERN),
      ConfigUtils.getOptionString(conf, FORMAT_KEY).getOrElse(DEFAULT_FORMAT),
      ConfigUtils.getExtraOptions(conf, "option"),
      ConfigUtils.getOptionString(conf, MODE_KEY).getOrElse(DEFAULT_MODE),
      ConfigUtils.getOptionInt(conf, RECORDS_PER_PARTITION),
      ConfigUtils.getOptionBoolean(conf, SAVE_EMPTY_KEY).getOrElse(true),
      ConfigUtils.getOptionBoolean(conf, GENERATE_INFO_FILE_KEY).getOrElse(true),
      ConfigUtils.getOptionBoolean(conf, PREFER_ADD_PARTITION_KEY).getOrElse(false),
      ConfigUtils.getOptionString(conf, ENCELADUS_RUN_MAIN_CLASS_KEY).getOrElse(DEFAULT_ENCELADUS_RUN_MAIN_CLASS),
      ConfigUtils.getOptionString(conf, ENCELADUS_COMMAND_LINE_TEMPLATE_KEY).getOrElse(DEFAULT_ENCELADUS_COMMAND_LINE_TEMPLATE),
      ConfigUtils.getOptionString(conf, HIVE_DATABASE_KEY),
      ConfigUtils.getOptionString(conf, PUBLISH_PARTITION_TEMPLATE_KEY).getOrElse(DEFAULT_PUBLISH_PARTITION_TEMPLATE)
    )
  }
}
