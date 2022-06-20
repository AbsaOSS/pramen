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

case class TransformerContext(
                               factoryClass: String,
                               jobName: String,
                               schedule: Schedule,
                               inputTables: List[String],
                               outputTable: String,
                               outputInfoDateExpression: Option[String],
                               options: Map[String, String]
                             )

object TransformerContext {
  def fromConfig(conf: Config, prefix: String): TransformerContext = {
    val JOB_NAME = s"$prefix.job.name"
    val FACTORY_CLASS = s"$prefix.factory.class"
    val INPUT_TABLES = s"$prefix.input.tables"
    val OUTPUT_TABLE = s"$prefix.output.table"
    val OUTPUT_INFO_DATE_EXPR = s"$prefix.output.info.date.expr"

    val factoryClass = conf.getString(FACTORY_CLASS)
    val jobName = ConfigUtils.getOptionString(conf, JOB_NAME).getOrElse("Custom job")
    val schedule = Schedule.fromConfig(conf.getConfig(prefix))
    val inputTables = ConfigUtils.getOptListStrings(conf, INPUT_TABLES)
    val outputTable = conf.getString(OUTPUT_TABLE)
    val outputInfoDateExpr = ConfigUtils.getOptionString(conf, OUTPUT_INFO_DATE_EXPR)
    val options = ConfigUtils.getExtraOptions(conf, s"$prefix.option")

    TransformerContext(factoryClass, jobName, schedule, inputTables.toList, outputTable, outputInfoDateExpr, options)
  }
}
