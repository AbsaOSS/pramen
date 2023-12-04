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

package za.co.absa.pramen.core.app.config

import com.typesafe.config.Config
import za.co.absa.pramen.core.utils.ConfigUtils

import java.time.ZoneId

case class GeneralConfig(
                          timezoneId: ZoneId,
                          environmentName: String,
                          temporaryDirectory: Option[String],
                          enableMultipleJobsPerTable: Boolean
                        )

object GeneralConfig {
  val TIMEZONE_ID_KEY = "pramen.timezone"
  val ENVIRONMENT_NAME_KEY = "pramen.environment.name"
  val TEMPORARY_DIRECTORY_KEY = "pramen.temporary.directory"
  val ENABLE_MULTIPLE_JOBS_PER_OUTPUT_TABLE = "pramen.enable.multiple.jobs.per.output.table"

  def fromConfig(conf: Config): GeneralConfig = {
    val timezoneId = ConfigUtils.getOptionString(conf, TIMEZONE_ID_KEY)
      .map(tz => ZoneId.of(tz))
      .getOrElse(ZoneId.systemDefault())
    val environmentName = conf.getString(ENVIRONMENT_NAME_KEY)
    val temporaryDirectory = ConfigUtils.getOptionString(conf, TEMPORARY_DIRECTORY_KEY)
    val enableMultipleJobsPerTable = conf.getBoolean(ENABLE_MULTIPLE_JOBS_PER_OUTPUT_TABLE)

    GeneralConfig(timezoneId, environmentName, temporaryDirectory, enableMultipleJobsPerTable)
  }
}
