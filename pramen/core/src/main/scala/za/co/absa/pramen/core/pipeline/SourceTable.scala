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
import za.co.absa.pramen.core.model.QueryBuilder
import za.co.absa.pramen.core.pipeline.OperationDef.WARN_MAXIMUM_EXECUTION_TIME_SECONDS_KEY
import za.co.absa.pramen.core.utils.{AlgorithmUtils, ConfigUtils}

import scala.collection.JavaConverters._

case class SourceTable(
                        metaTableName: String,
                        query: Query,
                        conf: Config,
                        rangeFromExpr: Option[String],
                        rangeToExpr: Option[String],
                        warnMaxExecutionTimeSeconds: Option[Int],
                        transformations: Seq[TransformExpression],
                        filters: Seq[String],
                        columns: Seq[String],
                        overrideConf: Option[Config] // ToDo: Add support for arbitrary read options passed to Spark (for cases like mergeSchema etc)
                      )

object SourceTable {
  private val log = LoggerFactory.getLogger(this.getClass)

  val METATABLE_TABLE_KEY = "output.metastore.table"
  val DATE_FROM_KEY = "date.from"
  val DATE_TO_KEY = "date.to"
  val COLUMNS_KEY = "columns"
  val TRANSFORMATIONS_KEY = "transformations"
  val FILTERS_KEY = "filters"
  val SOURCE_OVERRIDE_PREFIX = "source"

  def fromConfigSingleEntry(conf: Config, parentPath: String): SourceTable = {
    if (!conf.hasPath(METATABLE_TABLE_KEY)) {
      throw new IllegalArgumentException(s"'$METATABLE_TABLE_KEY' not set for '$parentPath' in the configuration.")
    }

    val metaTableName = conf.getString(METATABLE_TABLE_KEY)
    val query = QueryBuilder.fromConfig(conf, "input", parentPath)
    val dateFromExpr = ConfigUtils.getOptionString(conf, DATE_FROM_KEY)
    val dateToExpr = ConfigUtils.getOptionString(conf, DATE_TO_KEY)
    val maximumExecutionTimeSeconds = ConfigUtils.getOptionInt(conf, WARN_MAXIMUM_EXECUTION_TIME_SECONDS_KEY)
    val columns = ConfigUtils.getOptListStrings(conf, COLUMNS_KEY)
    val transformations = TransformExpression.fromConfig(conf, TRANSFORMATIONS_KEY, parentPath)
    val filters = ConfigUtils.getOptListStrings(conf, FILTERS_KEY)

    val overrideConf = if (conf.hasPath(SOURCE_OVERRIDE_PREFIX)) {
      log.info(s"Source table $metaTableName has a config override.")
      Some(conf.getConfig(SOURCE_OVERRIDE_PREFIX))
    } else {
      None
    }

    SourceTable(metaTableName, query, conf, dateFromExpr, dateToExpr, maximumExecutionTimeSeconds, transformations, filters, columns, overrideConf)
  }

  def fromConfig(conf: Config, arrayPath: String): Seq[SourceTable] = {
    val tableConfigs = conf.getConfigList(arrayPath).asScala

    val sourceTables = tableConfigs
      .zipWithIndex
      .map { case (tableConfig, idx) => fromConfigSingleEntry(tableConfig, s"$arrayPath[$idx]") }

    val duplicates = AlgorithmUtils.findDuplicates(sourceTables.map(_.metaTableName).toSeq)
    if (duplicates.nonEmpty) {
      throw new IllegalArgumentException(s"Duplicate source table definitions for the sourcing job: ${duplicates.mkString(", ")}")
    }
    sourceTables.toSeq
  }
}
