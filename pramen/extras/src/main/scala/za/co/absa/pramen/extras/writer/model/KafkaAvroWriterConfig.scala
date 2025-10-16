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

case class KafkaAvroWriterConfig(
                                  kafkaAvroConfig: KafkaAvroConfig,
                                  keyColumns: Seq[String],
                                  keySchemaId: Option[Int],
                                  valueSchemaId: Option[Int],
                                  recordsLimit: Option[Int]
                                )

object KafkaAvroWriterConfig {
  val KAFKA_KEY_COLUMN_NAMES = "key.column.names"
  val KAFKA_LIMIT_RECORDS = "limit.records"
  val SCHEMA_REGISTRY_KEY_SCHEMA_ID = "schema.registry.key.schema.id"
  val SCHEMA_REGISTRY_VALUE_SCHEMA_ID = "schema.registry.value.schema.id"

  def fromConfig(conf: Config): KafkaAvroWriterConfig = {
    val kafkaConfig = KafkaAvroConfig.fromConfig(conf)

    if (kafkaConfig.keyNamingStrategy.nonEmpty && !conf.hasPath(KAFKA_KEY_COLUMN_NAMES)) {
      throw new IllegalArgumentException(s"If key strategy is defined, column names must be define too. " +
        s"Please, define '<job>.key.column.names'")
    }

    if (kafkaConfig.keyNamingStrategy.isEmpty && conf.hasPath(KAFKA_KEY_COLUMN_NAMES)) {
      throw new IllegalArgumentException(s"If key columns are defined, naming strategy for keys need to be defined too. " +
        s"Please, define '<job>.schema.registry.key.naming.strategy'")
    }

    KafkaAvroWriterConfig(
      kafkaConfig,
      keyColumns = ConfigUtils.getOptListStrings(conf, KAFKA_KEY_COLUMN_NAMES),
      keySchemaId = ConfigUtils.getOptionInt(conf, SCHEMA_REGISTRY_KEY_SCHEMA_ID),
      valueSchemaId = ConfigUtils.getOptionInt(conf, SCHEMA_REGISTRY_VALUE_SCHEMA_ID),
      recordsLimit = ConfigUtils.getOptionInt(conf, KAFKA_LIMIT_RECORDS)
    )
  }
}
