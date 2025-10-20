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

package za.co.absa.pramen.extras.source

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.pramen.api.offset.OffsetValue.{KAFKA_OFFSET_FIELD, KAFKA_PARTITION_FIELD, KafkaValue}
import za.co.absa.pramen.api.offset.{OffsetInfo, OffsetType, OffsetValue}
import za.co.absa.pramen.api.{ExternalChannelFactoryV2, Query, Source, SourceResult}
import za.co.absa.pramen.extras.source.KafkaAvroSource.KAFKA_TOKENS_TO_REDACT
import za.co.absa.pramen.extras.utils.ConfigUtils
import za.co.absa.pramen.extras.writer.model.KafkaAvroConfig

import java.time.LocalDate

/**
  * A data source implementation for consuming data from Kafka topics using Avro serialization using Abris
  * and Confluent Schema Registry integration.
  *
  * This source leverages Spark's Kafka integration for batch reading to read messages from a Kafka topic and deserialize them
  * using the schema definitions from a Schema Registry. It is designed to work only in the incremental mode,
  * with offset management for as close to the exactly once processing as possible. Exactly once processing is possible
  * if the Kafka producer can guarantee that duplicate messages are not sent.
  *
  * Example source definition:
  * {{{
  *  {
  *    # Define a name to reference from the pipeline:
  *    name = "kafka_avro"
  *    factory.class = "za.co.absa.pramen.extras.source.KafkaAvroSource"
  *
  *    kafka {
  *      bootstrap.servers = "mybroker1:9092,mybroker2:9092"
  *
  *      # Arbitrary options for creating a Kafka Producer
  *      # sasl.jaas.config = "..."
  *      # sasl.mechanism = "..."
  *      # security.protocol = "..."
  *      # ...
  *    }
  *
  *    schema.registry {
  *      url = "https://my.schema.regictry:8081"
  *
  *      # Can be one of: topic.name, record.name, topic.record.name
  *      value.naming.strategy = "topic.name"
  *
  *      # If you want to force the specific schema id. Otherwise, the latest schema id will be used.
  *      # key.schema.id =
  *      # value.schema.id =
  *
  *      # Arbitrary options for the schema registry
  *      #  basic.auth.credentials.source = "..."
  *      #  basic.auth.user.info = "..."
  *      # ...
  *    }
  *  }
  * }}}
  *
  * Here is an example of a source definition in a pipeline.
  *
  * {{{
  *  {
  *    name = "Sourcing from a Kafka topic"
  *    type = "ingestion"
  *
  *    source = "kafka_avro"
  *
  *    schedule.type = "incremental"
  *
  *    tables = [
  *      {
  *        input.table = "my.topic"
  *        output.metastore.table = "my_table1"
  *      }
  *    ]
  *  }
  * }}}
  *
  * @param sourceConfig    The configuration block that defines the source (it is needed to fulfill the interface).
  * @param workflowConfig  The configuration of the whole workflow.
  * @param kafkaAvroConfig The Kafka connection configuration with Schema Registry and Avro options.
  * @param spark           A Spark session.
  */
class KafkaAvroSource(sourceConfig: Config,
                      workflowConfig: Config,
                      val kafkaAvroConfig: KafkaAvroConfig)
                     (implicit spark: SparkSession) extends Source {
  override def hasInfoDateColumn(query: Query): Boolean = false

  override def getOffsetInfo: Option[OffsetInfo] = {
    Some(OffsetInfo("kafka_offset", OffsetType.KafkaType))
  }

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    throw new IllegalArgumentException("KafkaAvroSource does not support batch jobs. Only incremental jobs are supported.")
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult = {
    getDataIncremental(query, None, None, None, Seq.empty)
  }

  override def getDataIncremental(query: Query,
                                  onlyForInfoDate: Option[LocalDate],
                                  offsetFromOpt: Option[OffsetValue],
                                  offsetToOpt: Option[OffsetValue],
                                  columns: Seq[String]): SourceResult = {
    if (onlyForInfoDate.isDefined)
      throw new IllegalArgumentException("KafkaAvroSource does not support information date fields.")

    val topic = query match {
      case t: Query.Table => t.dbTable
      case q: Query       => throw new IllegalArgumentException(s"KafkaAvroSource supports only 'table', got ${q.name}")
    }

    val q = "\""

    val startingOffsets = offsetFromOpt match {
      case Some(offset) =>
        // The starting offset is inclusive in Spark.
        if (offset.dataType != OffsetType.KafkaType)
          throw new IllegalArgumentException(s"KafkaAvroSource supports only 'kafka' offsets, got ${offset.dataType.dataTypeString}")

        // If 'to' part of the interval is defined, use the closed interval min <= offset <= max, otherwise min < offset.
        val offsetFrom = if (offsetToOpt.isDefined)
          offset
        else
          offset.asInstanceOf[KafkaValue].increment

        Map("startingOffsets" -> s"{$q$topic$q: ${offsetFrom.valueString}}")
      case None         =>
        Map("startingOffsets" -> "earliest")
    }

    val endingOffsets = offsetToOpt match {
      case Some(offset) =>
        // The ending offset is exclusive in Spark.
        if (offset.dataType != OffsetType.KafkaType)
          throw new IllegalArgumentException(s"KafkaAvroSource supports only 'kafka' offsets, got ${offset.dataType.dataTypeString}")
        val offsetTo = offset.asInstanceOf[KafkaValue].increment
        Map("endingOffsets" -> s"{$q$topic$q: ${offsetTo.valueString}}")
      case None         =>
        Map("endingOffsets" -> "latest")
    }

    val kafkaOptions = kafkaAvroConfig.extraOptions ++ startingOffsets ++ endingOffsets +
      ("kafka.bootstrap.servers" -> kafkaAvroConfig.brokers) +
      ("subscribe" -> topic)

    ConfigUtils.logExtraOptions("Options passed to the Kafka reader:",
      kafkaOptions,
      KAFKA_TOKENS_TO_REDACT
    )

    val dfRaw = spark.read
      .format("kafka")
      .options(kafkaOptions)
      .load()

    val schemaRegistryClientConfig = Map(
      AbrisConfig.SCHEMA_REGISTRY_URL -> kafkaAvroConfig.schemaRegistryUrl
    ) ++ kafkaAvroConfig.schemaRegistryExtraOptions

    // ToDo Add support for other naming strategy and for key deserialization
    val abrisConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(topic, isKey = false)
      .usingSchemaRegistry(schemaRegistryClientConfig)

    // Deserialize from Avro
    val df = dfRaw
      .withColumn("data", from_avro(col("value"), abrisConfig))
      .withColumn(KAFKA_PARTITION_FIELD, col("partition"))
      .withColumn(KAFKA_OFFSET_FIELD, col("offset"))
      .withColumn("kafka_timestamp", col("timestamp"))
      .withColumn("kafka_timestamp_type", col("timestampType"))
      .withColumn("kafka_key", col("key"))
      .select(KAFKA_PARTITION_FIELD, KAFKA_OFFSET_FIELD, "kafka_timestamp", "kafka_timestamp_type", "kafka_key", "data.*")

    df.printSchema()

    SourceResult(df)
  }

  override def config: Config = sourceConfig
}

object KafkaAvroSource extends ExternalChannelFactoryV2[KafkaAvroSource] {
  val TOPIC_NAME_KEY = "topic.name"

  val KAFKA_TOKENS_TO_REDACT = Set("password", "jaas.config", "auth.user.info")

  override def apply(conf: Config, workflowConfig: Config, parentPath: String, spark: SparkSession): KafkaAvroSource = {
    val kafkaReaderConfig = KafkaAvroConfig.fromConfig(conf)
    new KafkaAvroSource(conf, workflowConfig, kafkaReaderConfig)(spark)
  }
}
