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

package za.co.absa.pramen.core.pipeline

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.Query
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.config.InfoDateOverride
import za.co.absa.pramen.core.metastore.model.{DataFormat, MetaTable}
import za.co.absa.pramen.core.model.Constants.DATE_FORMAT_INTERNAL
import za.co.absa.pramen.core.model.QueryBuilder
import za.co.absa.pramen.core.utils.DateUtils.convertStrToDate
import za.co.absa.pramen.core.utils.{AlgorithmicUtils, ConfigUtils}

import java.time.LocalDate
import scala.collection.JavaConverters._

case class TransferTable(
                          query: Query,
                          jobMetaTableName: String,
                          rangeFromExpr: Option[String],
                          rangeToExpr: Option[String],
                          infoDateStart: LocalDate,
                          trackDays: Int,
                          transformations: Seq[TransformExpression],
                          filters: Seq[String],
                          columns: Seq[String],
                          readOptions: Map[String, String],
                          writeOptions: Map[String, String],
                          sourceOverrideConf: Option[Config],
                          sinkOverrideConf: Option[Config]
                        ) {
  def getSourceTable: SourceTable = {
    SourceTable(jobMetaTableName, query, rangeFromExpr, rangeToExpr, transformations, filters, columns, sourceOverrideConf)
  }

  def getSinkTable: SinkTable = {
    SinkTable(jobMetaTableName, Option(jobMetaTableName), rangeFromExpr, rangeToExpr, transformations, filters, columns, writeOptions, sinkOverrideConf)
  }

  def getMetaTable: MetaTable = {
    MetaTable(jobMetaTableName, "", DataFormat.Null(), "", "", None, None, infoDateStart, trackDays, readOptions, writeOptions)
  }
}

object TransferTable {
  private val log = LoggerFactory.getLogger(this.getClass)

  val JOB_METASTORE_OUTPUT_TABLE_KEY = "job.metastore.table"
  val DATE_FROM_KEY = "date.from"
  val DATE_TO_KEY = "date.to"
  val COLUMNS_KEY = "columns"
  val TRANSFORMATIONS_KEY = "transformations"
  val FILTERS_KEY = "filters"
  val SOURCE_OVERRIDE_PREFIX = "source"
  val SINK_OVERRIDE_PREFIX = "sink"
  val TRACK_DAYS_KEY = "track.days"

  def fromConfigSingleEntry(conf: Config, parentPath: String, sinkName: String, defaultStartDate: LocalDate, defaultTrackDays: Int): TransferTable = {
    val query = QueryBuilder.fromConfig(conf, "input", parentPath)
    val jobMetaTableOpt = ConfigUtils.getOptionString(conf, JOB_METASTORE_OUTPUT_TABLE_KEY)
    val dateFromExpr = ConfigUtils.getOptionString(conf, DATE_FROM_KEY)
    val dateToExpr = ConfigUtils.getOptionString(conf, DATE_TO_KEY)
    val trackDays = ConfigUtils.getOptionInt(conf, TRACK_DAYS_KEY).getOrElse(defaultTrackDays)
    val columns = ConfigUtils.getOptListStrings(conf, COLUMNS_KEY)
    val transformations = TransformExpression.fromConfig(conf, TRANSFORMATIONS_KEY, parentPath)
    val filters = ConfigUtils.getOptListStrings(conf, FILTERS_KEY)
    val readOptions = ConfigUtils.getExtraOptions(conf, "read.option")
    val writeOptions = ConfigUtils.getExtraOptions(conf, "output")

    val outputMetaTableName = jobMetaTableOpt.getOrElse(s"$query -> $sinkName")

    val sourceOverrideConf = if (conf.hasPath(SOURCE_OVERRIDE_PREFIX)) {
      log.info(s"Transfer table '$outputMetaTableName' has a source config override.")
      Some(conf.getConfig(SOURCE_OVERRIDE_PREFIX))
    } else {
      None
    }

    val sinkOverrideConf = if (conf.hasPath(SINK_OVERRIDE_PREFIX)) {
      log.info(s"Transfer table '$outputMetaTableName' has a sink config override.")
      Some(conf.getConfig(SINK_OVERRIDE_PREFIX))
    } else {
      None
    }

    val infoDateOverride = InfoDateOverride.fromConfig(conf)
    val startDate = infoDateOverride.startDate.getOrElse(defaultStartDate)
    val jobMetaTable = getOutputTableName(jobMetaTableOpt, query, sinkName)

    TransferTable(query, jobMetaTable, dateFromExpr, dateToExpr, startDate, trackDays, transformations, filters, columns, readOptions, writeOptions, sourceOverrideConf, sinkOverrideConf)
  }

  def fromConfig(conf: Config, appConfig: Config, arrayPath: String, sinkName: String): Seq[TransferTable] = {
    val defaultInfoDateFormat = appConfig.getString(InfoDateConfig.INFORMATION_DATE_FORMAT_KEY)
    val defaultStartDate = convertStrToDate(appConfig.getString(InfoDateConfig.INFORMATION_DATE_START_KEY), DATE_FORMAT_INTERNAL, defaultInfoDateFormat)
    val defaultTrackDays = appConfig.getInt(InfoDateConfig.TRACK_DAYS)

    val tableConfigs = conf.getConfigList(arrayPath).asScala

    val transferTables = tableConfigs
      .zipWithIndex
      .map { case (tableConfig, idx) => fromConfigSingleEntry(tableConfig, s"$arrayPath[$idx]", sinkName, defaultStartDate, defaultTrackDays) }

    val duplicates = AlgorithmicUtils.findDuplicates(transferTables.map(_.jobMetaTableName).toSeq)
    if (duplicates.nonEmpty) {
      throw new IllegalArgumentException(s"Duplicate table definitions for the transfer job: ${duplicates.mkString(", ")}")
    }
    transferTables.toSeq
  }

  private[core] def getInputTableName(query: Query): Option[String] = {
    query match {
      case t: Query.Table => Option(t.dbTable)
      case _              => None
    }
  }

  private[core] def getOutputTableName(jobMetaTableOpt: Option[String], query: Query, sinkName: String): String = {
    jobMetaTableOpt match {
      case Some(name) => name
      case None       =>
        getInputTableName(query) match {
          case Some(name) => s"$name->$sinkName"
          case None       => throw new IllegalArgumentException(s"Cannot determine metastore table name for '$query -> $sinkName'." +
            s"Please specify it explicitly via '$JOB_METASTORE_OUTPUT_TABLE_KEY'.")
        }
    }
  }
}