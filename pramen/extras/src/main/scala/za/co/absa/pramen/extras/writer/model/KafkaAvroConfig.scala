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

package za.co.absa.pramen.extras.writer.model

import com.typesafe.config.Config
import za.co.absa.pramen.extras.utils.ConfigUtils

case class KafkaAvroConfig(
                            brokers: String,
                            schemaRegistryUrl: String,
                            keyNamingStrategy: Option[NamingStrategy],
                            valueNamingStrategy: NamingStrategy,
                            extraOptions: Map[String, String],
                            schemaRegistryExtraOptions: Map[String, String]
                          )

object KafkaAvroConfig {
  val KAFKA_BROKERS_KEY = "kafka.bootstrap.servers"

  // Schema registry options
  val SCHEMA_REGISTRY_URL = "schema.registry.url"
  val SCHEMA_REGISTRY_KEY_PREFIX = "schema.registry.key"
  val SCHEMA_REGISTRY_VALUE_PREFIX = "schema.registry.value"
  val SCHEMA_EXTRA_OPTIONS = "schema.registry"
  val KAFKA_EXTRA_OPTIONS = "kafka"

  def fromConfig(conf: Config): KafkaAvroConfig = {
    val keyNamingStrategy = if (conf.hasPath(SCHEMA_REGISTRY_KEY_PREFIX)) {
      NamingStrategy.fromConfigOpt(conf.getConfig(SCHEMA_REGISTRY_KEY_PREFIX))
    } else {
      None
    }

    val valueNamingStrategy = NamingStrategy.fromConfigOpt(conf.getConfig(SCHEMA_REGISTRY_VALUE_PREFIX))

    if (valueNamingStrategy.isEmpty) {
      throw new IllegalArgumentException(s"Value naming strategy is not defined. " +
        s"Please, define '<job>.schema.registry.value.naming.strategy'")
    }

    KafkaAvroConfig(
      brokers = conf.getString(KAFKA_BROKERS_KEY),
      schemaRegistryUrl = conf.getString(SCHEMA_REGISTRY_URL),
      keyNamingStrategy = keyNamingStrategy,
      valueNamingStrategy = valueNamingStrategy.get,
      ConfigUtils.getExtraOptions(conf, KAFKA_EXTRA_OPTIONS),
      ConfigUtils.getExtraOptions(conf, SCHEMA_EXTRA_OPTIONS)
    )
  }
}
