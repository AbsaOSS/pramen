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
import za.co.absa.pramen.api.offset.{OffsetType, OffsetValue}
import za.co.absa.pramen.api.{ExternalChannelFactoryV2, Query, Source, SourceResult}
import za.co.absa.pramen.extras.writer.model.KafkaConfig

import java.time.LocalDate

class KafkaAvroSource(sourceConfig: Config,
                      workflowConfig: Config,
                      val kafkaConfig: KafkaConfig)
                     (implicit spark: SparkSession) extends Source {
  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    throw new IllegalArgumentException("KafkaAvroSource does not support batch jobs. Only incremental jobs are supported.")
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult = {
    throw new IllegalArgumentException("KafkaAvroSource does not support batch jobs. Only incremental jobs are supported.")
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
      case q: Query => throw new IllegalArgumentException(s"KafkaAvroSource supports only 'table', got ${q.name}")
    }

    val startingOffsets = offsetFromOpt match {
      case Some(offset) =>
        if (offset.dataType != OffsetType.StringType)
          throw new IllegalArgumentException(s"KafkaAvroSource supports only 'string' offsets, got ${offset.dataType.dataTypeString}")
        Map("startingOffsets" -> offset.valueString)
      case None => Map("startingOffsets" -> "earliest")
    }

    val endingOffsets = offsetToOpt match {
      case Some(offset) =>
        if (offset.dataType != OffsetType.StringType)
          throw new IllegalArgumentException(s"KafkaAvroSource supports only 'string' offsets, got ${offset.dataType.dataTypeString}")
        Map("endingOffsets" -> offset.valueString)
      case None => Map("endingOffsets" -> "latest")
    }

    val dfRaw = spark.read
      .format("kafka")
      .options(kafkaConfig.extraOptions)
      .option("subscribe", topic)
      .options(startingOffsets)
      .options(endingOffsets)
      .load()

    val abrisConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(topic, isKey = false)
      .usingSchemaRegistry(kafkaConfig.schemaRegistryExtraOptions)

    // Deserialize from Avro
    val df = dfRaw
      .withColumn("data", from_avro(col("value"), abrisConfig))
      .withColumn("kafka_partition", col("partition"))
      .withColumn("kafka_offset", col("kafka_partition"))
      .withColumn("kafka_timestamp", col("timestamp"))
      .withColumn("kafka_timestamp_type", col("timestampType"))
      .select("kafka_partition", "kafka_offset", "kafka_timestamp", "kafka_timestamp_type", "data.*")

    SourceResult(df)
  }

  override def config: Config = sourceConfig
}

object KafkaAvroSource extends ExternalChannelFactoryV2[KafkaAvroSource] {
  val TOPIC_NAME_KEY = "topic.name"

  override def apply(conf: Config, workflowConfig: Config, parentPath: String, spark: SparkSession): KafkaAvroSource = {
    val kafkaReaderConfig = KafkaConfig.fromConfig(conf, isWriter = false)
    new KafkaAvroSource(conf, workflowConfig, kafkaReaderConfig)(spark)
  }
}
