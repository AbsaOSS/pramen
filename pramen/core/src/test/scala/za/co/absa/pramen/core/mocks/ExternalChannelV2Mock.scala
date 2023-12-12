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

package za.co.absa.pramen.core.mocks

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api.{ExternalChannel, ExternalChannelFactoryV2}

class ExternalChannelV2Mock(conf: Config, val workflowConfig: Config, val value1: String, val value2: String) extends ExternalChannel {
  def config: Config = conf
}

object ExternalChannelV2Mock extends ExternalChannelFactoryV2[ExternalChannelV2Mock] {
  val CONFIG_KEY1 = "key1"
  val CONFIG_KEY2 = "key2"

  override def apply(conf: Config, workflowConfig: Config, parentPath: String, spark: SparkSession): ExternalChannelV2Mock = {
    val value1 = conf.getString(CONFIG_KEY1)
    val value2 = conf.getString(CONFIG_KEY2)
    new ExternalChannelV2Mock(conf, workflowConfig, value1, value2)
  }
}
