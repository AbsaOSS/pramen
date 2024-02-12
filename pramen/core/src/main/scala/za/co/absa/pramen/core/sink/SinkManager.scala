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

package za.co.absa.pramen.core.sink

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api.Sink
import za.co.absa.pramen.core.ExternalChannelFactoryReflect

object SinkManager {
  val SINKS_KEY = "pramen.sinks"

  /**
    * Get sink object from the list of sink from the workflow config.
    *
    * @param name         The name of the sink.
    * @param conf         The workflow configuration.
    * @param overrideConf The config override, of defined.
    * @return The sink object.
    */
  def getSinkByName(name: String, conf: Config, overrideConf: Option[Config])(implicit spark: SparkSession): Sink = {
    ExternalChannelFactoryReflect.fromConfigByName[Sink](conf, overrideConf, SINKS_KEY, name, "sink")
  }
}
