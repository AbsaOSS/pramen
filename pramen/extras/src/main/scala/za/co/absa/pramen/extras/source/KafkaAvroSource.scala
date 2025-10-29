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
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.pramen.api.offset.OffsetValue.KafkaValue
import za.co.absa.pramen.api.offset.{OffsetInfo, OffsetType, OffsetValue}
import za.co.absa.pramen.api.{ExternalChannelFactoryV2, Query, Source, SourceResult}
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
  *    # [Optional] Set name for the struct field that contains Kafka record metadata
  *    custom.kafka.column = "kafka"
  *
  *    # [Optional] Set name for the Kafka key column
  *    key.column.name = "kafka_key"
  *
  *    # The Kafka key serializer when 'key.naming.strategy' is NOT defined. Can be "none", "binary", "string".
  *    # When 'key.naming.strategy' IS defined in 'schema.registry', Avro deserialization is used automatically.
  *    # Default is "binary".
  *    #key.column.serializer = "none"
  *
  *    kafka {
  *      bootstrap.servers = "mybroker1:9092,mybroker2:9092"
  *
  *      # Arbitrary options for the Kafka consumer/reader
  *      # sasl.jaas.config = "..."
  *      # sasl.mechanism = "..."
  *      # security.protocol = "..."
  *      # ...
  *    }
  *
  *    schema.registry {
  *      url = "https://my.schema.registry:8081"
  *
  *      # Can be one of: topic.name, record.name, topic.record.name
  *      value.naming.strategy = "topic.name"
  *      #key.naming.strategy = "topic.name"
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
  import za.co.absa.pramen.extras.source.KafkaAvroSource._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val kafkaColumnName = ConfigUtils.getOptionString(sourceConfig, CUSTOM_KAFKA_COLUMN_KEY).getOrElse("kafka")
  private val keyColumnName = ConfigUtils.getOptionString(sourceConfig, KEY_COLUMN_KEY).getOrElse("kafka_key")
  private val keyColumnSerializer = ConfigUtils.getOptionString(sourceConfig, KEY_COLUMN_SERIALIZER_KEY).getOrElse("binary").toLowerCase.trim
  private val tempKafkaColumnName = "tmp_pramen_kafka"
  private val tempKafkaKeyColumnName = "tmp_pramen_kafka_key"

  override def hasInfoDateColumn(query: Query): Boolean = false

  override def getOffsetInfo: Option[OffsetInfo] = {
    Some(OffsetInfo(kafkaColumnName, OffsetType.KafkaType))
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

    val abrisValueBase = AbrisConfig
      .fromConfluentAvro.downloadReaderSchemaByLatestVersion

    val abrisValueConfig = kafkaAvroConfig.valueNamingStrategy
      .applyNamingStrategyToAbrisConfig(abrisValueBase, topic, isKey = false)
      .usingSchemaRegistry(schemaRegistryClientConfig)

    val df1 = dfRaw
      .withColumn("data", from_avro(col("value"), abrisValueConfig))
      .withColumn("tmp_pramen_kafka", struct(
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("timestampType").as("timestamp_type")
      ))

    val hasKey = kafkaAvroConfig.keyNamingStrategy.isDefined || keyColumnSerializer != "none"

    val df2 = kafkaAvroConfig.keyNamingStrategy match {
      case Some(keyNamingStrategy) =>
        val abrisKeyConfig = keyNamingStrategy
          .applyNamingStrategyToAbrisConfig(abrisValueBase, topic, isKey = true)
          .usingSchemaRegistry(schemaRegistryClientConfig)
        df1.withColumn(tempKafkaKeyColumnName, from_avro(col("key"), abrisKeyConfig))
      case None =>
        keyColumnSerializer match {
          case "none" => df1
          case "binary" => df1.withColumn(tempKafkaKeyColumnName, col("key"))
          case "string" => df1.withColumn(tempKafkaKeyColumnName, col("key").cast(StringType))
          case "avro" => throw new IllegalArgumentException("For the 'avro' serializer of Kafka topic key, 'schema.registry.key.naming.strategy' needs to be set.")
          case x => throw new IllegalArgumentException(s"Unknown Kafka key serializer '$x'. Can be one of: none, binary, string, avro.")
        }
    }

    val payloadFields = df2.select("data.*").schema.fieldNames.toSet
    if (payloadFields.contains(kafkaColumnName)) {
      log.warn(s"Payload field '$kafkaColumnName' conflicts with reserved Kafka metadata struct name and will be replaced.")
    }
    if (payloadFields.contains(keyColumnName)) {
      log.warn(s"Payload field '$keyColumnName' conflicts with reserved Kafka key column name and will be removed.")
    }

    // Put data fields to the root level of the schema, and if data struct already has kafka_key and kafka fields,
    // drop them
    val dfFinal = if (hasKey) {
      df2.select(tempKafkaKeyColumnName, "data.*", tempKafkaColumnName)
        .drop(kafkaColumnName)
        .drop(keyColumnName)
        .withColumnRenamed(tempKafkaColumnName, kafkaColumnName)
        .withColumnRenamed(tempKafkaKeyColumnName, keyColumnName)
    } else {
      df2.select("data.*", tempKafkaColumnName)
        .drop(kafkaColumnName)
        .drop(keyColumnName)
        .withColumnRenamed(tempKafkaColumnName, kafkaColumnName)
    }

    SourceResult(dfFinal)
  }

  override def config: Config = sourceConfig
}

object KafkaAvroSource extends ExternalChannelFactoryV2[KafkaAvroSource] {
  val TOPIC_NAME_KEY = "topic.name"
  val CUSTOM_KAFKA_COLUMN_KEY = "custom.kafka.column"
  val KEY_COLUMN_KEY = "key.column.name"
  val KEY_COLUMN_SERIALIZER_KEY = "key.column.serializer"

  val KAFKA_TOKENS_TO_REDACT = Set("password", "jaas.config", "auth.user.info")

  override def apply(conf: Config, workflowConfig: Config, parentPath: String, spark: SparkSession): KafkaAvroSource = {
    val kafkaReaderConfig = KafkaAvroConfig.fromConfig(conf)
    new KafkaAvroSource(conf, workflowConfig, kafkaReaderConfig)(spark)
  }
}
