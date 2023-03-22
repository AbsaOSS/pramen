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

package za.co.absa.pramen.extras.sink

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.{ExternalChannelFactory, MetastoreReader, Sink, SinkResult}
import za.co.absa.pramen.extras.sink.KafkaAvroSink.TOPIC_NAME_KEY
import za.co.absa.pramen.extras.writer.TableWriterKafka
import za.co.absa.pramen.extras.writer.model.KafkaWriterConfig

import java.time.LocalDate

/**
  * This sink allows exporting data from the metastore to a Kafka topic in Avro format.
  *
  * In order to use the sink you need to define sink parameters.
  *
  * Example sink definition:
  * {{{
  *  {
  *    # Define a name to reference from the pipeline:
  *    name = "kafka_avro"
  *    factory.class = "za.co.absa.pramen.extras.sink.KafkaAvroSink"
  *
  *    writer.kafka {
  *      brokers = "mybroker1:9092,mybroker2:9092"
  *      schema.registry.url = "https://my.schema.regictry:8081"
  *
  *      # Can be one of: topic.name, record.name, topic.record.name
  *      schema.registry.value.naming.strategy = "topic.name"
  *
  *      # Arbitrary options for creating a Kafka Producer
  *      option {
  *        kafka.sasl.jaas.config = "..."
  *        kafka.sasl.mechanism = "..."
  *        kafka.security.protocol = "..."
  *        # ...
  *      }
  *
  *      # Arbitrary options for Schema registry
  *      schema.registry.option {
  *        basic.auth.credentials.source = "..."
  *        basic.auth.user.info = "..."
  *        # ...
  *      }
  *    }
  *  }
  * }}}
  *
  * Here is an example of a sink definition in a pipeline. As for any other operation you can specify
  * dependencies, transformations, filters and columns to select.
  *
  * {{{
  *  {
  *    name = "Kafka sink"
  *    type = "sink"
  *    sink = "kafka_avro"
  *
  *    schedule.type = "daily"
  *
  *    # Optional dependencies
  *    dependencies = [
  *      {
  *        tables = [ dependent_table ]
  *        date.from = "@infoDate"
  *      }
  *    ]
  *
  *    tables = [
  *      {
  *        metastore.table = metastore_table
  *        output.topic.name = "my.topic"
  *
  *        # All following settings are OPTIONAL
  *
  *        # Date range to read the source table for. By default the job information date is used.
  *        # But you can define an arbitrary expression based on the information date.
  *        # More: see the section of documentation regarding date expressions, an the list of functions allowed.
  *        date {
  *          from = "@infoDate"
  *          to = "@infoDate"
  *        }
  *
  *        transformations = [
  *         { col = "col1", expr = "lower(some_string_column)" }
  *        ],
  *        filters = [
  *          "some_numeric_column > 100"
  *        ]
  *        columns = [ "col1", "col2", "col2", "some_numeric_column" ]
  *      }
  *    ]
  *  }
  * }}}
  *
  */
class KafkaAvroSink(sinkConfig: Config,
                    val kafkaWriterConfig: KafkaWriterConfig) extends Sink {

  override val config: Config = sinkConfig
  override def connect(): Unit = {}

  override def close(): Unit = {}

  override def send(df: DataFrame,
                    tableName: String,
                    metastore: MetastoreReader,
                    infoDate: LocalDate,
                    options: Map[String, String])(implicit spark: SparkSession): SinkResult = {
    if (!options.contains(TOPIC_NAME_KEY)) {
      throw new IllegalArgumentException(s"$TOPIC_NAME_KEY is not specified for Kafka sink, table: $tableName")
    }
    val topicName = options(TOPIC_NAME_KEY)

    val writer = new TableWriterKafka(topicName, kafkaWriterConfig, kafkaWriterConfig.extraOptions ++ options)

    SinkResult(writer.write(df, infoDate, None))
  }
}

object KafkaAvroSink extends ExternalChannelFactory[KafkaAvroSink] {
  val TOPIC_NAME_KEY = "topic.name"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): KafkaAvroSink = {
    val kafkaWriterConfig = KafkaWriterConfig.fromConfig(conf)
    new KafkaAvroSink(conf, kafkaWriterConfig)
  }
}
