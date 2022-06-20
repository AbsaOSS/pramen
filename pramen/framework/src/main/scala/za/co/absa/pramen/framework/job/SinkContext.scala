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

case class SinkContext(
                        jobName: String,
                        schedule: Schedule,
                        sinkName: String,
                        tables: List[SinkTable]
                      )

object SinkContext {
  def fromConfig(conf: Config, prefix: String): SinkContext = {
    val JOB_NAME = s"$prefix.job.name"
    val SINK_KEY = s"$prefix.sink"
    val INPUT_TABLES = s"$prefix.tables"

    val jobName = ConfigUtils.getOptionString(conf, JOB_NAME).getOrElse("Sink job")
    val sinkName = conf.getString(SINK_KEY)
    val schedule = Schedule.fromConfig(conf.getConfig(prefix))
    val inputTables = SinkTable.fromConfig(conf, INPUT_TABLES)

    SinkContext(jobName, schedule, sinkName, inputTables.toList)
  }
}
