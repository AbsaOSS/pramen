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

case class NamingStrategy(
                         namingStrategy: String,
                         recordName: Option[String],
                         recordNamespace: Option[String]
                         ) {
  import NamingStrategy._

  def needsRecordName: Boolean = {
    namingStrategy == NAMING_STRATEGY_RECORD_NAME ||
      namingStrategy == NAMING_STRATEGY_TOPIC_RECORD_NAME
  }

  def getSubject(topicName: String, isKey: Boolean): SchemaSubject = {
    if (namingStrategy == NAMING_STRATEGY_TOPIC_NAME) {
      SchemaSubject.usingTopicNameStrategy(topicName, isKey)
    } else if (namingStrategy == NAMING_STRATEGY_RECORD_NAME) {
      SchemaSubject.usingRecordNameStrategy(recordName.get, recordNamespace.get)
    } else if (namingStrategy == NAMING_STRATEGY_TOPIC_RECORD_NAME) {
      SchemaSubject.usingTopicRecordNameStrategy(topicName, recordName.get, recordNamespace.get)
    } else {
      throw new IllegalArgumentException(s"Unknown naming strategy: $namingStrategy")
    }
  }
}

object NamingStrategy {
  private val NAMING_STRATEGY_TOPIC_NAME = "topic.name"
  private val NAMING_STRATEGY_RECORD_NAME = "record.name"
  private val NAMING_STRATEGY_TOPIC_RECORD_NAME = "topic.record.name"

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