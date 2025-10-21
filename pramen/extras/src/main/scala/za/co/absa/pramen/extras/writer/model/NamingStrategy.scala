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
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.config.{FromSchemaDownloadingConfigFragment, FromStrategyConfigFragment}

case class NamingStrategy(
                           namingStrategy: String,
                           recordName: Option[String],
                           recordNamespace: Option[String]
                         ) {

  import NamingStrategy._

  /**
    * Determines whether the naming strategy requires the use of a record name for schema subject resolution.
    */
  def needsRecordName: Boolean = {
    namingStrategy == NAMING_STRATEGY_RECORD_NAME ||
      namingStrategy == NAMING_STRATEGY_TOPIC_RECORD_NAME
  }

  /**
    * Determines the schema subject to use based on the specified topic name,
    * whether the subject is for a key or value, and the naming strategy.
    *
    * @param topicName The name of the Kafka topic.
    * @param isKey     A flag indicating whether the subject is for a key (true) or a value (false).
    * @return A `SchemaSubject` instance derived according to the naming strategy and parameters.
    */
  def getSubject(topicName: String, isKey: Boolean): SchemaSubject = {
    namingStrategy match {
      case NAMING_STRATEGY_TOPIC_NAME        =>
        SchemaSubject.usingTopicNameStrategy(topicName, isKey)
      case NAMING_STRATEGY_RECORD_NAME       =>
        val (name, ns) = getNameAndNamespace(NAMING_STRATEGY_RECORD_NAME)
        SchemaSubject.usingRecordNameStrategy(name, ns)
      case NAMING_STRATEGY_TOPIC_RECORD_NAME =>
        val (name, ns) = getNameAndNamespace(NAMING_STRATEGY_TOPIC_RECORD_NAME)
        SchemaSubject.usingTopicRecordNameStrategy(topicName, name, ns)
      case _                                 =>
        throw new IllegalArgumentException(s"Unknown naming strategy: $namingStrategy")
    }
  }

  /**
    * Applies a naming strategy to the given Abris configuration, based on the specified topic name
    * and key flag. Determines the appropriate naming strategy to use and modifies the configuration
    * accordingly.
    *
    * Example:
    * {{{
    *   val abrisValueBase = AbrisConfig.fromConfluentAvro.downloadReaderSchemaByLatestVersion
    *
    *   val abrisValueConfig = kafkaAvroConfig.valueNamingStrategy
    *     .applyNamingStrategyToAbrisConfig(abrisValueBase, topic, isKey = false)
    *     .usingSchemaRegistry(schemaRegistryClientConfig)
    * }}}
    *
    * @param abrisConfig The initial Abris configuration to which the naming strategy will be applied.
    * @param topicName   The name of the topic for which the naming strategy is being applied.
    * @param isKey       A flag indicating whether the naming strategy is for a key or a value.
    * @return A modified Abris configuration with the applied naming strategy.
    */
  def applyNamingStrategyToAbrisConfig(abrisConfig: FromStrategyConfigFragment, topicName: String, isKey: Boolean): FromSchemaDownloadingConfigFragment = {
    val partStr = if (isKey) "key" else "value"
    namingStrategy match {
      case NAMING_STRATEGY_TOPIC_NAME =>
        abrisConfig.andTopicNameStrategy(topicName, isKey)
      case NAMING_STRATEGY_RECORD_NAME =>
        (recordName, recordNamespace) match {
          case (Some(name), Some(namespace)) =>
            abrisConfig.andRecordNameStrategy(name, namespace)
          case _ =>
            throw new IllegalArgumentException(s"Record name and namespace must be defined for $partStr naming strategy '$NAMING_STRATEGY_RECORD_NAME'")
        }
      case NAMING_STRATEGY_TOPIC_RECORD_NAME =>
        (recordName, recordNamespace) match {
          case (Some(name), Some(namespace)) =>
            abrisConfig.andTopicRecordNameStrategy(topicName, name, namespace)
          case _ =>
            throw new IllegalArgumentException(s"Record name and namespace must be defined for $partStr naming strategy '$NAMING_STRATEGY_TOPIC_RECORD_NAME'")
        }
      case other =>
        throw new IllegalArgumentException(s"Unsupported $partStr naming strategy: $other")
    }
  }

  private def getNameAndNamespace(strategy: String): (String, String) = {
    (recordName.getOrElse(throw new IllegalArgumentException(s"Record name missing for the subject naming strategy '$strategy'.")),
      recordNamespace.getOrElse(throw new IllegalArgumentException(s"Record namespace missing for the subject naming strategy '$strategy'.")))
  }
}

object NamingStrategy {
  val NAMING_STRATEGY_TOPIC_NAME = "topic.name"
  val NAMING_STRATEGY_RECORD_NAME = "record.name"
  val NAMING_STRATEGY_TOPIC_RECORD_NAME = "topic.record.name"

  val NAMING_STRATEGY = "naming.strategy"
  val SCHEMA_RECORD_NAME = "schema.record.name"
  val SCHEMA_RECORD_NAMESPACE = "schema.record.namespace"

  def fromConfigOpt(conf: Config): Option[NamingStrategy] = {
    if (conf.hasPath(NAMING_STRATEGY)) {
      val namingStrategy = conf.getString(NAMING_STRATEGY)

      Option(namingStrategy match {
        case NAMING_STRATEGY_TOPIC_NAME =>
          NamingStrategy(namingStrategy,
            None,
            None)
        case NAMING_STRATEGY_RECORD_NAME =>
          NamingStrategy(namingStrategy,
            Option(conf.getString(SCHEMA_RECORD_NAME)),
            Option(conf.getString(SCHEMA_RECORD_NAMESPACE)))
        case NAMING_STRATEGY_TOPIC_RECORD_NAME =>
          NamingStrategy(namingStrategy,
            Option(conf.getString(SCHEMA_RECORD_NAME)),
            Option(conf.getString(SCHEMA_RECORD_NAMESPACE)))
        case _ => throw new IllegalArgumentException(s"Illegal naming strategy '$namingStrategy'. " +
          s"Can be one of: '$NAMING_STRATEGY_TOPIC_NAME', '$NAMING_STRATEGY_RECORD_NAME' or '$NAMING_STRATEGY_TOPIC_RECORD_NAME'.")
      })
    } else {
      None
    }
  }
}
