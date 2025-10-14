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

case class KafkaConfig(
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

object KafkaConfig {
  val KAFKA_WRITER_PREFIX = "writer.kafka"
  val KAFKA_READER_PREFIX = "reader.kafka"

  def KAFKA_WRITER_BROKERS_KEY(prefix: String) = s"$prefix.brokers"

  def KAFKA_WRITER_SCHEMA_REGISTRY_URL(prefix: String) = s"$prefix.schema.registry.url"

  def KAFKA_WRITER_KEY_COLUMN_NAMES(prefix: String) = s"$prefix.key.column.names"

  def KAFKA_WRITER_LIMIT_RECORDS(prefix: String) = s"$prefix.limit.records"

  def KAFKA_WRITER_EXTRA_OPTIONS(prefix: String) = s"$prefix.option"

  def SCHEMA_REGISTRY_EXTRA_OPTIONS(prefix: String) = s"$prefix.schema.registry.option"

  def SCHEMA_REGISTRY_KEY_PREFIX(prefix: String) = s"$prefix.schema.registry.key"

  def SCHEMA_REGISTRY_VALUE_PREFIX(prefix: String) = s"$prefix.schema.registry.value"

  def KAFKA_WRITER_KEY_SCHEMA_ID(prefix: String) = s"$prefix.schema.registry.key.schema.id"

  def KAFKA_WRITER_KEY_VALUE_ID(prefix: String) = s"$prefix.schema.registry.value.schema.id"

  def fromConfig(conf: Config, isWriter: Boolean): KafkaConfig = {
    val prefix = if (isWriter) {
      KAFKA_WRITER_PREFIX
    } else {
      KAFKA_READER_PREFIX
    }
    val keyNamingStrategy = if (conf.hasPath(SCHEMA_REGISTRY_KEY_PREFIX(prefix))) {
      NamingStrategy.fromConfigOpt(conf.getConfig(SCHEMA_REGISTRY_KEY_PREFIX(prefix)))
    } else {
      None
    }

    if (keyNamingStrategy.nonEmpty && !conf.hasPath(KAFKA_WRITER_KEY_COLUMN_NAMES(prefix))) {
      throw new IllegalArgumentException(s"If key strategy is defined, column names must be define too. " +
        s"Please, define '<job>.$KAFKA_WRITER_PREFIX.key.column.names'")
    }

    if (keyNamingStrategy.isEmpty && conf.hasPath(KAFKA_WRITER_KEY_COLUMN_NAMES(prefix))) {
      throw new IllegalArgumentException(s"If key columns are defined, naming strategy for keys need to be defined too. " +
        s"Please, define '<job>.$KAFKA_WRITER_PREFIX.schema.registry.key.naming.strategy'")
    }

    val valueNamingStrategy = NamingStrategy.fromConfigOpt(conf.getConfig(SCHEMA_REGISTRY_VALUE_PREFIX(prefix)))

    if (valueNamingStrategy.isEmpty) {
      throw new IllegalArgumentException(s"Value naming strategy is not defined. " +
        s"Please, define '<job>.$KAFKA_WRITER_PREFIX.schema.registry.value.naming.strategy'")
    }

    KafkaConfig(
      brokers = conf.getString(KAFKA_WRITER_BROKERS_KEY(prefix)),
      schemaRegistryUrl = conf.getString(KAFKA_WRITER_SCHEMA_REGISTRY_URL(prefix)),
      keyColumns = ConfigUtils.getOptListStrings(conf, KAFKA_WRITER_KEY_COLUMN_NAMES(prefix)),
      keyNamingStrategy = keyNamingStrategy,
      valueNamingStrategy = valueNamingStrategy.get,
      keySchemaId = ConfigUtils.getOptionInt(conf, KAFKA_WRITER_KEY_SCHEMA_ID(prefix)),
      valueSchemaId = ConfigUtils.getOptionInt(conf, KAFKA_WRITER_KEY_VALUE_ID(prefix)),
      recordsLimit = ConfigUtils.getOptionInt(conf, KAFKA_WRITER_LIMIT_RECORDS(prefix)),
      ConfigUtils.getExtraOptions(conf, KAFKA_WRITER_EXTRA_OPTIONS(prefix)),
      ConfigUtils.getExtraOptions(conf, SCHEMA_REGISTRY_EXTRA_OPTIONS(prefix))
    )
  }

}
