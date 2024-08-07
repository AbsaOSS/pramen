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

package za.co.absa.pramen.core.metastore.model

import com.typesafe.config.Config
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{DataFormat, MetaTableDef}
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.config.InfoDateOverride
import za.co.absa.pramen.core.utils.{AlgorithmUtils, ConfigUtils}

import java.time.LocalDate
import scala.util.{Failure, Success, Try}

/**
  * This is metatable details available to read from the metastore.
  *
  * @param name                   The name of the table.
  * @param description            The description of the table.
  * @param format                 The format of the table.
  * @param infoDateColumn         The name of the column that contains the information date (partitioned by).
  * @param infoDateFormat         The format of the information date.
  * @param hiveConfig             The effective Hive configuration to use for Hive operations.
  * @param hiveTable              The name of the Hive table.
  * @param hivePath               The path of the Hive table (if it differs from the path in the underlying format).
  * @param hivePreferAddPartition If true, prefer ADD PARTITION to MSCK REPAIR when possible for Hive updates.
  * @param infoDateExpression     The expression to use to calculate the information date.
  * @param infoDateStart          The start date of the information date.
  * @param trackDays              The number of days to look back for retrospective changes if this table is used as a dependency.
  * @param trackDaysExplicitlySet if true, trackDays was set explicitly. If false, trackDays is taken from workflow defaults.
  * @param readOptions            The read options for the table.
  * @param writeOptions           The write options for the table.
  * @param sparkConfig            Special Spark configuration to use when writing to the table.
  */
case class MetaTable(
                      name: String,
                      description: String,
                      format: DataFormat,
                      infoDateColumn: String,
                      infoDateFormat: String,
                      hiveConfig: HiveConfig,
                      hiveTable: Option[String],
                      hivePath: Option[String],
                      hivePreferAddPartition: Boolean,
                      infoDateExpression: Option[String],
                      infoDateStart: LocalDate,
                      trackDays: Int,
                      trackDaysExplicitlySet: Boolean,
                      saveModeOpt: Option[SaveMode],
                      readOptions: Map[String, String],
                      writeOptions: Map[String, String],
                      sparkConfig: Map[String, String]
                    )

object MetaTable {
  private val log = LoggerFactory.getLogger(this.getClass)

  val NAME_KEY = "name"
  val NAME_DESCRIPTION = "description"
  val HIVE_TABLE_KEY = "hive.table"
  val HIVE_PATH_KEY = "hive.path"
  val HIVE_PREFER_ADD_PARTITION_KEY = "hive.prefer.add.partition"
  val TRACK_DAYS_KEY = "track.days"
  val SAVE_MODE_OPTION_KEY = "save.mode"
  val READ_OPTION_KEY = "read.option"
  val WRITE_OPTION_KEY = "write.option"
  val TABLE_HIVE_CONFIG_PREFIX = "hive"
  val DEFAULT_HIVE_CONFIG_PREFIX = "pramen.hive"
  val SPARK_CONFIG_PREFIX = "spark.conf"

  def fromConfig(conf: Config, infoDateConfig: InfoDateConfig, key: String): Seq[MetaTable] = {
    val defaultInfoDateColumnName = infoDateConfig.columnName
    val defaultInfoDateFormat = infoDateConfig.dateFormat
    val defaultStartDate = infoDateConfig.startDate
    val defaultTrackDays = infoDateConfig.defaultTrackDays
    val defaultHiveConfig = HiveDefaultConfig.fromConfig(ConfigUtils.getOptionConfig(conf, DEFAULT_HIVE_CONFIG_PREFIX))
    val defaultPreferAddPartition = conf.getBoolean(s"pramen.$HIVE_PREFER_ADD_PARTITION_KEY")

    val tableConfigs = ConfigUtils.getOptionConfigList(conf, key)

    if (tableConfigs.isEmpty) {
      log.warn(s"Config key '$key' not found. The metastore has no tables. The pipeline can run only if it consists of only transfer operations.")
    }

    val metatables = tableConfigs
      .map(tableConfig => fromConfigSingleEntity(tableConfig, conf, defaultInfoDateColumnName, defaultInfoDateFormat, defaultStartDate, defaultTrackDays, defaultHiveConfig, defaultPreferAddPartition))
      .toSeq

    val duplicates = AlgorithmUtils.findDuplicates(metatables.map(_.name))
    if (duplicates.nonEmpty) {
      throw new IllegalArgumentException(s"Duplicate table definitions in the metastore: ${duplicates.mkString(", ")}")
    }
    metatables
  }

  def fromConfigSingleEntity(conf: Config,
                             appConf: Config,
                             defaultInfoColumnName: String,
                             defaultInfoDateFormat: String,
                             defaultStartDate: LocalDate,
                             defaultTrackDays: Int,
                             defaultHiveConfig: HiveDefaultConfig,
                             defaultPreferAddPartition: Boolean): MetaTable = {
    val name = ConfigUtils.getOptionString(conf, NAME_KEY).getOrElse(throw new IllegalArgumentException(s"Mandatory option missing: $NAME_KEY"))
    val description = ConfigUtils.getOptionString(conf, NAME_DESCRIPTION).getOrElse("")
    val infoDateOverride = InfoDateOverride.fromConfig(conf)
    val infoDateColumn = infoDateOverride.columnName.getOrElse(defaultInfoColumnName)
    val infoDateFormat = infoDateOverride.dateFormat.getOrElse(defaultInfoDateFormat)
    val infoDateExpressionOpt = infoDateOverride.expression
    val startDate = infoDateOverride.startDate.getOrElse(defaultStartDate)
    val trackDays = ConfigUtils.getOptionInt(conf, TRACK_DAYS_KEY).getOrElse(defaultTrackDays)
    val trackDaysExplicitlySet = conf.hasPath(TRACK_DAYS_KEY)

    val format = Try {
      DataFormatParser.fromConfig(conf, appConf)
    } match {
      case Success(f) => f
      case Failure(ex) => throw new IllegalArgumentException(s"Unable to read data format from config for the metastore table: $name", ex)
    }

    val hiveTable = ConfigUtils.getOptionString(conf, HIVE_TABLE_KEY)
    val hivePath = ConfigUtils.getOptionString(conf, HIVE_PATH_KEY)
    val hivePreferAddPartition = ConfigUtils.getOptionBoolean(conf, HIVE_PREFER_ADD_PARTITION_KEY).getOrElse(defaultPreferAddPartition)

    val hiveConfig = if (hiveTable.isEmpty) {
      HiveConfig.fromDefaults(defaultHiveConfig, format)
    } else {
      HiveConfig.fromConfigWithDefaults(ConfigUtils.getOptionConfig(conf, TABLE_HIVE_CONFIG_PREFIX), defaultHiveConfig, format)
    }

    val saveModeOpt = ConfigUtils.getOptionString(conf, SAVE_MODE_OPTION_KEY).map(getSaveMode(_, name))
    val readOptions = ConfigUtils.getExtraOptions(conf, READ_OPTION_KEY)
    val writeOptions = ConfigUtils.getExtraOptions(conf, WRITE_OPTION_KEY)
    val sparkConfig = ConfigUtils.getExtraOptions(conf, SPARK_CONFIG_PREFIX)

    MetaTable(name,
      description,
      format,
      infoDateColumn,
      infoDateFormat,
      hiveConfig,
      hiveTable,
      hivePath,
      hivePreferAddPartition,
      infoDateExpressionOpt,
      startDate,
      trackDays,
      trackDaysExplicitlySet,
      saveModeOpt,
      readOptions,
      writeOptions,
      sparkConfig)
  }

  def getMetaTableDef(table: MetaTable): MetaTableDef = {
    MetaTableDef(
      table.name,
      table.description,
      table.format,
      table.infoDateColumn,
      table.infoDateFormat,
      table.hiveTable,
      table.hivePath,
      table.infoDateStart,
      table.readOptions,
      table.writeOptions
    )
  }

  private[core] def getSaveMode(saveModeStr: String, table: String): SaveMode = {
    saveModeStr.toLowerCase.trim match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case _ => throw new IllegalArgumentException(s"Invalid or unsupported save mode: '$saveModeStr' for table '$table'.")
    }
  }
}
