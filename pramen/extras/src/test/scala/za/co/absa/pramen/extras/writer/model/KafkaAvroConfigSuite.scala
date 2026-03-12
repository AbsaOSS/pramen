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

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec

class KafkaAvroConfigSuite extends AnyWordSpec {
  "fromConfig" should {
    "read a minimalistic config with brokers and schema registry URL" in {
      val conf = ConfigFactory.parseString(
        """kafka.bootstrap.servers = "localhost:9092"
          |schema.registry.url = "localhost:8081"
          |schema.registry.value.naming.strategy = "topic.name"
          |""".stripMargin)

      val kafkaConfig = KafkaAvroConfig.fromConfig(conf)

      assert(kafkaConfig.brokers == "localhost:9092")
      assert(kafkaConfig.schemaRegistryUrl == "localhost:8081")
      assert(kafkaConfig.valueNamingStrategy.namingStrategy == "topic.name")
      assert(kafkaConfig.keyNamingStrategy.isEmpty)
    }

    "read a config with key naming strategy" in {
      val conf = ConfigFactory.parseString(
        """kafka.bootstrap.servers = "localhost:9092"
          |schema.registry.url = "localhost:8081"
          |schema.registry.value.naming.strategy = "topic.name"
          |schema.registry.key.naming.strategy = "topic.name"
          |""".stripMargin)

      val kafkaConfig = KafkaAvroConfig.fromConfig(conf)

      assert(kafkaConfig.brokers == "localhost:9092")
      assert(kafkaConfig.schemaRegistryUrl == "localhost:8081")
      assert(kafkaConfig.valueNamingStrategy.namingStrategy == "topic.name")
      assert(kafkaConfig.keyNamingStrategy.isDefined)
      assert(kafkaConfig.keyNamingStrategy.get.namingStrategy == "topic.name")
    }

    "read a config with record.name naming strategy" in {
      val conf = ConfigFactory.parseString(
        """kafka.bootstrap.servers = "localhost:9092"
          |schema.registry.url = "localhost:8081"
          |schema.registry.value {
          |  naming.strategy = "record.name"
          |  schema.record.name = "MyRecord"
          |  schema.record.namespace = "com.example"
          |}
          |""".stripMargin)

      val kafkaConfig = KafkaAvroConfig.fromConfig(conf)

      assert(kafkaConfig.brokers == "localhost:9092")
      assert(kafkaConfig.schemaRegistryUrl == "localhost:8081")
      assert(kafkaConfig.valueNamingStrategy.namingStrategy == "record.name")
      assert(kafkaConfig.valueNamingStrategy.recordName.contains("MyRecord"))
      assert(kafkaConfig.valueNamingStrategy.recordNamespace.contains("com.example"))
    }

    "read a config with extra auth properties" in {
      val conf = ConfigFactory.parseString(
        """kafka.bootstrap.servers = "localhost:9092"
          |schema.registry.url = "localhost:8081"
          |schema.registry.value.naming.strategy = "topic.name"
          |schema.registry.option {
          |  basic.auth.credentials.source = "USER_INFO"
          |  basic.auth.user.info = "test:12345"
          |  ssl.truststore.location = "cacerts"
          |  ssl.truststore.type = "JKS"
          |}
          |""".stripMargin)

      val kafkaConfig = KafkaAvroConfig.fromConfig(conf)

      assert(kafkaConfig.brokers == "localhost:9092")
      assert(kafkaConfig.schemaRegistryUrl == "localhost:8081")
      assert(kafkaConfig.valueNamingStrategy.namingStrategy == "topic.name")
      assert(kafkaConfig.keyNamingStrategy.isEmpty)
      assert(kafkaConfig.schemaRegistryExtraOptions("basic.auth.credentials.source") == "USER_INFO")
      assert(kafkaConfig.schemaRegistryExtraOptions("basic.auth.user.info") == "test:12345")
      assert(kafkaConfig.schemaRegistryExtraOptions("ssl.truststore.location") == "cacerts")
      assert(kafkaConfig.schemaRegistryExtraOptions("ssl.truststore.type") == "JKS")
    }

    "throw an exception when bootstrap servers are missing" in {
      val conf = ConfigFactory.parseString(
        """schema.registry.url = "localhost:8081"
          |schema.registry.value.naming.strategy = "topic.name"
          |""".stripMargin)

      intercept[com.typesafe.config.ConfigException.Missing] {
        KafkaAvroConfig.fromConfig(conf)
      }
    }

    "throw an exception when schema registry URL is missing" in {
      val conf = ConfigFactory.parseString(
        """kafka.bootstrap.servers = "localhost:9092"
          |schema.registry.value.naming.strategy = "topic.name"
          |""".stripMargin)

      intercept[com.typesafe.config.ConfigException.Missing] {
        KafkaAvroConfig.fromConfig(conf)
      }
    }

    "throw an exception when value naming strategy is missing" in {
      val conf = ConfigFactory.parseString(
        """kafka.bootstrap.servers = "localhost:9092"
          |schema.registry.url = "localhost:8081"
          |""".stripMargin)

      intercept[com.typesafe.config.ConfigException.Missing] {
        KafkaAvroConfig.fromConfig(conf)
      }
    }
  }
}
