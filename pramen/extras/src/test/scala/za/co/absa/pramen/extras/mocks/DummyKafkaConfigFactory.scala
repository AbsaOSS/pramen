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

package za.co.absa.pramen.extras.mocks

import za.co.absa.pramen.extras.writer.model.{KafkaAvroConfig, KafkaAvroWriterConfig, NamingStrategy}

object DummyKafkaConfigFactory {
  def getDummyKafkaAvroConfig(brokers: String = "localhost:9092",
                              schemaRegistryUrl: String = "localhost:8081",
                              keyNamingStrategy: Option[NamingStrategy] = None,
                              valueNamingStrategy: NamingStrategy = DummyNamingStrategyFactory.getDummyNamingStrategy(),
                              extraOptions: Map[String, String] = Map(),
                              schemaRegistryExtraOptions: Map[String, String] = Map()): KafkaAvroConfig = {
    KafkaAvroConfig(
      brokers = brokers,
      schemaRegistryUrl = schemaRegistryUrl,
      keyNamingStrategy = keyNamingStrategy,
      valueNamingStrategy = valueNamingStrategy,
      extraOptions = extraOptions,
      schemaRegistryExtraOptions
    )
  }

  def getDummyKafkaWriterConfig(kafkaAvroConfig: KafkaAvroConfig = getDummyKafkaAvroConfig(),
                                keyColumns: Seq[String] = Nil,
                                keySchemaId: Option[Int] = None,
                                valueSchemaId: Option[Int] = None,
                                recordsLimit: Option[Int] = None,
                               ): KafkaAvroWriterConfig = {
    KafkaAvroWriterConfig(
      kafkaAvroConfig = kafkaAvroConfig,
      keyColumns = keyColumns,
      keySchemaId = keySchemaId,
      valueSchemaId = valueSchemaId,
      recordsLimit = recordsLimit
    )
  }
}
