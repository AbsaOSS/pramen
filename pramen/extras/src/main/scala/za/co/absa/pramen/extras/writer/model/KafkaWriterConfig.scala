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

case class KafkaWriterConfig(
                              brokers: String,
                              schemaRegistryUrl: String,
                              keyColumns: Seq[String],
                              keyNamingStrategy: Option[NamingStrategy],
                              valueNamingStrategy: NamingStrategy,
                              keySchemaId: Option[Int],
                              valueSchemaId: Option[Int],
                              recordsLimit: Option[Int],
                              extraOptions: Map[String, String],
                              schemaRegistryExtraOptions: Map[String, String]
                            )

object KafkaWriterConfig {
  val KAFKA_WRITER_PREFIX = "writer.kafka"
  val KAFKA_WRITER_BROKERS_KEY = s"$KAFKA_WRITER_PREFIX.brokers"
  val KAFKA_WRITER_SCHEMA_REGISTRY_URL = s"$KAFKA_WRITER_PREFIX.schema.registry.url"
  val KAFKA_WRITER_KEY_COLUMN_NAMES = s"$KAFKA_WRITER_PREFIX.key.column.names"
  val KAFKA_WRITER_LIMIT_RECORDS = s"$KAFKA_WRITER_PREFIX.limit.records"

  val KAFKA_WRITER_EXTRA_OPTIONS = s"$KAFKA_WRITER_PREFIX.option"
  val SCHEMA_REGISTRY_EXTRA_OPTIONS = s"$KAFKA_WRITER_PREFIX.schema.registry.option"

  val SCHEMA_REGISTRY_KEY_PREFIX = s"$KAFKA_WRITER_PREFIX.schema.registry.key"
  val SCHEMA_REGISTRY_VALUE_PREFIX = s"$KAFKA_WRITER_PREFIX.schema.registry.value"

  val KAFKA_WRITER_KEY_SCHEMA_ID = s"$SCHEMA_REGISTRY_KEY_PREFIX.schema.id"
  val KAFKA_WRITER_KEY_VALUE_ID = s"$SCHEMA_REGISTRY_VALUE_PREFIX.schema.id"

  def fromConfig(conf: Config): KafkaWriterConfig = {
    val keyNamingStrategy = if (conf.hasPath(SCHEMA_REGISTRY_KEY_PREFIX)) {
      NamingStrategy.fromConfigOpt(conf.getConfig(SCHEMA_REGISTRY_KEY_PREFIX))
    } else {
      None
    }

    if (keyNamingStrategy.nonEmpty && !conf.hasPath(KAFKA_WRITER_KEY_COLUMN_NAMES)) {
      throw new IllegalArgumentException(s"If key strategy is defined, column names must be define too. " +
        s"Please, define '<job>.$KAFKA_WRITER_PREFIX.key.column.names'")
    }

    if (keyNamingStrategy.isEmpty && conf.hasPath(KAFKA_WRITER_KEY_COLUMN_NAMES)) {
      throw new IllegalArgumentException(s"If key columns are defined, naming strategy for keys need to be defined too. " +
        s"Please, define '<job>.$KAFKA_WRITER_PREFIX.schema.registry.key.naming.strategy'")
    }

    val valueNamingStrategy = NamingStrategy.fromConfigOpt(conf.getConfig(SCHEMA_REGISTRY_VALUE_PREFIX))

    if (valueNamingStrategy.isEmpty) {
      throw new IllegalArgumentException(s"Value naming strategy is not defined. " +
        s"Please, define '<job>.$KAFKA_WRITER_PREFIX.schema.registry.value.naming.strategy'")
    }

    KafkaWriterConfig(
      brokers = conf.getString(KAFKA_WRITER_BROKERS_KEY),
      schemaRegistryUrl = conf.getString(KAFKA_WRITER_SCHEMA_REGISTRY_URL),
      keyColumns = ConfigUtils.getOptListStrings(conf, KAFKA_WRITER_KEY_COLUMN_NAMES),
      keyNamingStrategy = keyNamingStrategy,
      valueNamingStrategy = valueNamingStrategy.get,
      keySchemaId = ConfigUtils.getOptionInt(conf, KAFKA_WRITER_KEY_SCHEMA_ID),
      valueSchemaId = ConfigUtils.getOptionInt(conf, KAFKA_WRITER_KEY_VALUE_ID),
      recordsLimit = ConfigUtils.getOptionInt(conf, KAFKA_WRITER_LIMIT_RECORDS),
      ConfigUtils.getExtraOptions(conf, KAFKA_WRITER_EXTRA_OPTIONS),
      ConfigUtils.getExtraOptions(conf, SCHEMA_REGISTRY_EXTRA_OPTIONS)
    )
  }

}
