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

package za.co.absa.pramen.framework.pipeline

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.framework.utils.{AlgorithmicUtils, ConfigUtils}

import scala.collection.JavaConverters._

case class SinkTable(
                      metaTableName: String,
                      outputTableName: Option[String],
                      sinkFromExpr: Option[String],
                      sinkToExpr: Option[String],
                      transformations: Seq[TransformExpression],
                      filters: Seq[String],
                      columns: Seq[String],
                      options: Map[String, String],
                      overrideConf: Option[Config]
                    )

object SinkTable {
  private val log = LoggerFactory.getLogger(this.getClass)

  val METATABLE_TABLE_KEY = "input.metastore.table"
  val JOB_METASTORE_OUTPUT_TABLE_KEY = "job.metastore.table"
  val DATE_FROM_KEY = "date.from"
  val DATE_TO_KEY = "date.to"
  val TRANSFORMATIONS_KEY = "transformations"
  val FILTERS_KEY = "filters"
  val COLUMNS_KEY = "columns"
  val SINK_OVERRIDE_PREFIX = "sink"

  def fromConfigSingleEntry(conf: Config, parentPath: String): SinkTable = {
    if (!conf.hasPath(METATABLE_TABLE_KEY)) {
      throw new IllegalArgumentException(s"'$METATABLE_TABLE_KEY' not set for '$parentPath' in the configuration.")
    }

    val metaTableName = conf.getString(METATABLE_TABLE_KEY)
    val outputTableName = ConfigUtils.getOptionString(conf, JOB_METASTORE_OUTPUT_TABLE_KEY)
    val dateFromExpr = ConfigUtils.getOptionString(conf, DATE_FROM_KEY)
    val dateToExpr = ConfigUtils.getOptionString(conf, DATE_TO_KEY)
    val transformations = TransformExpression.fromConfig(conf, TRANSFORMATIONS_KEY, parentPath)
    val filters = ConfigUtils.getOptListStrings(conf, FILTERS_KEY)
    val columns = ConfigUtils.getOptListStrings(conf, COLUMNS_KEY)
    val options = ConfigUtils.getExtraOptions(conf, "output")

    val overrideConf = if (conf.hasPath(SINK_OVERRIDE_PREFIX)) {
      log.info(s"Sink table $metaTableName has a config override.")
      Some(conf.getConfig(SINK_OVERRIDE_PREFIX))
    } else {
      None
    }

    SinkTable(metaTableName, outputTableName, dateFromExpr, dateToExpr, transformations, filters, columns, options, overrideConf)
  }

  def fromConfig(conf: Config, arrayPath: String): Seq[SinkTable] = {
    val tableConfigs = conf.getConfigList(arrayPath).asScala

    val sinkTables = tableConfigs
      .zipWithIndex
      .map { case (tableConfig, idx) => fromConfigSingleEntry(tableConfig, s"$arrayPath[$idx]") }

    val duplicates = AlgorithmicUtils.findDuplicates(sinkTables.map(_.metaTableName))
    if (duplicates.nonEmpty) {
      throw new IllegalArgumentException(s"Duplicate sink table definitions for the sink job: ${duplicates.mkString(", ")}")
    }
    sinkTables
  }
}