/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.pramen.framework.metastore.model

import com.typesafe.config.Config
import za.co.absa.pramen.api.{MetaTable => MetaTableApi}
import za.co.absa.pramen.framework.app.config.InfoDateConfig
import za.co.absa.pramen.framework.config.InfoDateOverride
import za.co.absa.pramen.framework.model.Constants.DATE_FORMAT_INTERNAL
import za.co.absa.pramen.framework.utils.DateUtils.convertStrToDate
import za.co.absa.pramen.framework.utils.{AlgorithmicUtils, ConfigUtils}

import java.time.LocalDate
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.util.{Failure, Success, Try}

object MetaTable {
  val NAME_KEY = "name"
  val NAME_DESCRIPTION = "description"
  val HIVE_TABLE_KEY = "hive.table"
  val TRACK_DAYS_KEY = "track.days"
  val READ_OPTION_KEY = "read.option"
  val WRITE_OPTION_KEY = "write.option"

  def fromConfig(conf: Config, key: String): Seq[MetaTableApi] = {
    val defaultInfoDateColumnName = conf.getString(InfoDateConfig.INFORMATION_DATE_COLUMN_KEY)
    val defaultInfoDateFormat = conf.getString(InfoDateConfig.INFORMATION_DATE_FORMAT_KEY)
    val defaultStartDate = convertStrToDate(conf.getString(InfoDateConfig.INFORMATION_DATE_START_KEY), DATE_FORMAT_INTERNAL, defaultInfoDateFormat)
    val defaultTrackDays = conf.getInt(InfoDateConfig.TRACK_DAYS)

    val tableConfigs = conf.getConfigList(key).asScala

    val metatables = tableConfigs
      .map(tableConfig => fromConfigSingleEntity(tableConfig, defaultInfoDateColumnName, defaultInfoDateFormat, defaultStartDate, defaultTrackDays))
      .toSeq

    val duplicates = AlgorithmicUtils.findDuplicates(metatables.map(_.name))
    if (duplicates.nonEmpty) {
      throw new IllegalArgumentException(s"Duplicate table definitions in the metastore: ${duplicates.mkString(", ")}")
    }
    metatables
  }

  def fromConfigSingleEntity(conf: Config,
                             defaultInfoColumnName: String,
                             defaultInfoDateFormat: String,
                             defaultStartDate: LocalDate,
                             defaultTrackDays: Int): MetaTableApi = {
    val name = ConfigUtils.getOptionString(conf, NAME_KEY).getOrElse(throw new IllegalArgumentException(s"Mandatory option missing: $NAME_KEY"))
    val description = ConfigUtils.getOptionString(conf, NAME_DESCRIPTION).getOrElse("")
    val infoDateOverride = InfoDateOverride.fromConfig(conf)
    val infoDateColumn = infoDateOverride.columnName.getOrElse(defaultInfoColumnName)
    val infoDateFormat = infoDateOverride.dateFormat.getOrElse(defaultInfoDateFormat)
    val infoDateExpressionOpt = infoDateOverride.expression
    val startDate = infoDateOverride.startDate.getOrElse(defaultStartDate)
    val trackDays = ConfigUtils.getOptionInt(conf, TRACK_DAYS_KEY).getOrElse(defaultTrackDays)

    val format = Try {
      DataFormat.fromConfig(conf)
    } match {
      case Success(f) => f
      case Failure(ex) => throw new IllegalArgumentException(s"Unable to read data format from config for the metastore table: $name", ex)
    }

    val hiveTable = ConfigUtils.getOptionString(conf, HIVE_TABLE_KEY)
    val readOptions = ConfigUtils.getExtraOptions(conf, READ_OPTION_KEY)
    val writeOptions = ConfigUtils.getExtraOptions(conf, WRITE_OPTION_KEY)

    MetaTableApi(name, description, format, infoDateColumn, infoDateFormat, hiveTable, infoDateExpressionOpt, startDate, trackDays, readOptions, writeOptions)
  }

}
