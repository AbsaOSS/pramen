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

package za.co.absa.pramen.framework.job

import com.typesafe.config.Config
import za.co.absa.pramen.api.schedule.Schedule
import za.co.absa.pramen.framework.utils.ConfigUtils

case class IngestionContext(
                             jobName: String,
                             schedule: Schedule,
                             sourceName: String,
                             tables: List[SourceTable],
                             outputInfoDateExpression: Option[String],
                             processingTimestampCol: Option[String]
                           )

object IngestionContext {
  def fromConfig(conf: Config, prefix: String): IngestionContext = {
    val JOB_NAME = s"$prefix.job.name"
    val SOURCE_KEY = s"$prefix.source"
    val INPUT_TABLES = s"$prefix.tables"
    val OUTPUT_INFO_DATE_EXPR = s"$prefix.output.info.date.expr"
    val PROCESSING_TIMESTAMP_COL_KEY = s"$prefix.processing.timestamp.col"

    val jobName = ConfigUtils.getOptionString(conf, JOB_NAME).getOrElse("Sourcing job")
    val sourceName = conf.getString(SOURCE_KEY)
    val schedule = Schedule.fromConfig(conf.getConfig(prefix))
    val inputTables = SourceTable.fromConfig(conf, INPUT_TABLES)
    val outputInfoDateExpr = ConfigUtils.getOptionString(conf, OUTPUT_INFO_DATE_EXPR)
    val processingTimestampCol = ConfigUtils.getOptionString(conf, PROCESSING_TIMESTAMP_COL_KEY)

    IngestionContext(jobName, schedule, sourceName, inputTables.toList, outputInfoDateExpr, processingTimestampCol)
  }
}