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
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.metastore.model.MetastoreDependency
import za.co.absa.pramen.core.schedule.Schedule
import za.co.absa.pramen.core.utils.ConfigUtils

import scala.collection.JavaConverters._

/** This is a base class for all Pramen jobs (new API). */
case class OperationDef(
                         name: String,
                         operationConf: Config,
                         operationType: OperationType,
                         schedule: Schedule,
                         expectedDelayDays: Int,
                         dependencies: Seq[MetastoreDependency],
                         outputInfoDateExpression: String,
                         initialSourcingDateExpression: String,
                         processingTimestampColumn: Option[String],
                         schemaTransformations: Seq[TransformExpression],
                         filters: Seq[String],
                         extraOptions: Map[String, String]
                       )

object OperationDef {
  private val log = LoggerFactory.getLogger(this.getClass)

  val NAME_KEY = "name"
  val TYPE_KEY = "type"
  val DISABLED_KEY = "disabled"
  val SCHEDULE_KEY = "schedule"
  val EXPECTED_DELAY_DAYS_KEY = "expected.delay.days"
  val DEPENDENCIES_KEY = "dependencies"
  val OUTPUT_INFO_DATE_EXPRESSION_KEY = "info.date.expr"
  val INITIAL_SOURCING_DATE_EXPR = "initial.sourcing.date.expr"
  val PROCESSING_TIMESTAMP_COLUMN_KEY = "processing.timestamp.column"
  val SCHEMA_TRANSFORMATIONS_KEY = "transformations"
  val FILTERS_KEY = "filters"
  val EXTRA_OPTIONS_PREFIX = "option"

  def fromConfig(conf: Config, infoDateConfig: InfoDateConfig, parent: String, defaultDelayDays: Int): Option[OperationDef] = {
    ConfigUtils.validatePathsExistence(conf, parent, Seq(NAME_KEY, TYPE_KEY, SCHEDULE_KEY))

    val name = conf.getString(NAME_KEY)
    val disabled = ConfigUtils.getOptionBoolean(conf, DISABLED_KEY).getOrElse(false)

    if (disabled) {
      log.warn(s"Operation '$name' is DISABLED.")
      return None
    }

    val operationType = OperationType.fromConfig(conf, parent)
    val schedule = Schedule.fromConfig(conf)
    val expectedDelayDays = ConfigUtils.getOptionInt(conf, EXPECTED_DELAY_DAYS_KEY).getOrElse(defaultDelayDays)
    val dependencies = getDependencies(conf, parent)
    val outputInfoDateExpressionOpt = ConfigUtils.getOptionString(conf, OUTPUT_INFO_DATE_EXPRESSION_KEY)
    val initialSourcingDateExpressionOpt = ConfigUtils.getOptionString(conf, INITIAL_SOURCING_DATE_EXPR)
    val processingTimestampColumn = ConfigUtils.getOptionString(conf, PROCESSING_TIMESTAMP_COLUMN_KEY)
    val schemaTransformations = TransformExpression.fromConfig(conf, SCHEMA_TRANSFORMATIONS_KEY, parent)
    val filters = ConfigUtils.getOptListStrings(conf, FILTERS_KEY)
    val extraOptions = ConfigUtils.getExtraOptions(conf, EXTRA_OPTIONS_PREFIX)

    val outputInfoDateExpression = outputInfoDateExpressionOpt match {
      case Some(expr) => expr
      case None       =>
        schedule match {
          case _: Schedule.EveryDay => infoDateConfig.expressionDaily
          case _: Schedule.Weekly   => infoDateConfig.expressionWeekly
          case _: Schedule.Monthly  => infoDateConfig.expressionMonthly
        }
    }

    val initialSourcingDateExpression = initialSourcingDateExpressionOpt match {
      case Some(expr) => expr
      case None       =>
        schedule match {
          case _: Schedule.EveryDay => infoDateConfig.initialSourcingDateExprDaily
          case _: Schedule.Weekly   => infoDateConfig.initialSourcingDateExprWeekly
          case _: Schedule.Monthly  => infoDateConfig.initialSourcingDateExprMonthly
        }
    }

    Some(OperationDef(name,
      conf,
      operationType,
      schedule,
      expectedDelayDays,
      dependencies,
      outputInfoDateExpression,
      initialSourcingDateExpression,
      processingTimestampColumn,
      schemaTransformations,
      filters,
      extraOptions))
  }

  private def getDependencies(conf: Config, parent: String): Seq[MetastoreDependency] = {
    if (conf.hasPath(DEPENDENCIES_KEY)) {
      val dependencyConfigs = conf.getConfigList(DEPENDENCIES_KEY)
      dependencyConfigs.asScala
        .zipWithIndex
        .map { case (c, i) => MetastoreDependency.fromConfig(c, s"$parent[$i]") }
    } else {
      Seq.empty[MetastoreDependency]
    }
  }
}