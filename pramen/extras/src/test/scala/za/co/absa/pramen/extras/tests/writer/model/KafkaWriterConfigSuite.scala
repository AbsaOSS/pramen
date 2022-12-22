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

package za.co.absa.pramen.extras.tests.writer.model

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.wordspec.AnyWordSpec

class KafkaWriterConfigSuite extends AnyWordSpec {
  import za.co.absa.pramen.extras.writer.model.KafkaWriterConfig._

  private val conf = ConfigFactory.parseString(
    """writer.kafka.brokers = "localhost:9092"
      |writer.kafka.schema.registry.url = "localhost:8081"
      |writer.kafka.schema.registry.value.naming.strategy = "topic.name"
      |writer.kafka.schema.registry.key.naming.strategy = "topic.name"
      |writer.kafka.key.column.names = [ a, b ]
      |""".stripMargin)


  "fromConfig()" should {
    "read a minimalistic config" in {
      val kafkaConfig = fromConfig(conf
        .withoutPath("writer.kafka.schema.registry.key.naming.strategy")
      .withoutPath("writer.kafka.key.column.names"))

      assert(kafkaConfig.brokers == "localhost:9092")
      assert(kafkaConfig.schemaRegistryUrl == "localhost:8081")
      assert(kafkaConfig.valueNamingStrategy.namingStrategy == "topic.name")
    }

    "read a minimalistic config with extra params" in {
      val kafkaConfig = fromConfig(conf
        .withoutPath("writer.kafka.schema.registry.key.naming.strategy")
        .withoutPath("writer.kafka.key.column.names")
        .withValue("writer.kafka.schema.registry.key.naming", ConfigValueFactory.fromAnyRef("x")))

      assert(kafkaConfig.brokers == "localhost:9092")
      assert(kafkaConfig.schemaRegistryUrl == "localhost:8081")
      assert(kafkaConfig.valueNamingStrategy.namingStrategy == "topic.name")
    }

    "read a config with key schema strategy defined" when {
      "key column names are defined" in {
        val kafkaConfig = fromConfig(conf)

        assert(kafkaConfig.brokers == "localhost:9092")
        assert(kafkaConfig.schemaRegistryUrl == "localhost:8081")
        assert(kafkaConfig.valueNamingStrategy.namingStrategy == "topic.name")
        assert(kafkaConfig.keyNamingStrategy.isDefined)
      }

      "key column names are not defined" in {
        val ex = intercept[IllegalArgumentException] {
          fromConfig(conf.withoutPath("writer.kafka.key.column.names"))
        }

        assert(ex.getMessage.contains("column names must be define too"))
      }
    }

    "throw an exception when key columns are defined without key naming strategy" in {
      val ex = intercept[IllegalArgumentException] {
        fromConfig(conf.withoutPath("writer.kafka.schema.registry.key.naming.strategy"))
      }

      assert(ex.getMessage.contains("naming strategy for keys need to be defined too"))
    }


  }

}
