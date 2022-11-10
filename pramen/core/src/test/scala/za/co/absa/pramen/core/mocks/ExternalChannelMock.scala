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
import za.co.absa.pramen.api.{ExternalChannel, ExternalChannelFactory}

class ExternalChannelMock(conf: Config, val value1: String, val value2: String) extends ExternalChannel {
  def config: Config = conf
}

object ExternalChannelMock extends ExternalChannelFactory[ExternalChannelMock] {
  val CONFIG_KEY1 = "key1"
  val CONFIG_KEY2 = "key2"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): ExternalChannelMock = {
    val value1 = conf.getString(CONFIG_KEY1)
    val value2 = conf.getString(CONFIG_KEY2)
    new ExternalChannelMock(conf, value1, value2)
  }
}
