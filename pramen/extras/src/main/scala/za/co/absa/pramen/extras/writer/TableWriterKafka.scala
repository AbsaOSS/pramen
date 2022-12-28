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

package za.co.absa.pramen.extras.writer

import com.typesafe.config.Config
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig}
import za.co.absa.pramen.extras.avro.AvroUtils.{convertSparkToAvroSchema, fixNullableFields}
import za.co.absa.pramen.extras.utils.ConfigUtils
import za.co.absa.pramen.extras.writer.model.{KafkaWriterConfig, NamingStrategy}

import java.time.LocalDate

class TableWriterKafka(topicName: String,
                       kafkaConfig: KafkaWriterConfig,
                       extraOptions: Map[String, String])
                      (implicit spark: SparkSession) extends TableWriter {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def write(df: DataFrame, infoDate: LocalDate, numOfRecordsEstimate: Option[Long]): Long = {
    val dfOut = getOutputDataFrame(df)

    val count = dfOut.count()

    if (count > 0) {
      log.info(s"Writing $count records to '$topicName'...")

      dfOut.write
        .format("kafka")
        .option("topic", topicName)
        .option("kafka.bootstrap.servers", kafkaConfig.brokers)
        .options(extraOptions)
        .save()

      count
    } else {
      log.info(s"Nothing to write to '$topicName'...")
      0L
    }
  }

  private[pramen] def getExtraOptions = extraOptions

  private[pramen] def getOutputDataFrame(df: DataFrame): DataFrame = {
    val dfOut = kafkaConfig.keyNamingStrategy match {
      case Some(_) => getKeyValueDataFrame(df)
      case None    => getValueDataFrame(df)
    }

    kafkaConfig.recordsLimit match {
      case Some(limit) => dfOut.limit(limit)
      case None        => dfOut
    }
  }

  private[pramen] def getValueDataFrame(df: DataFrame): DataFrame = {
    val allColumns = struct(df.columns.map(c => df(c)): _*)

    val valueAvroConfig = getAvroConfig(allColumns, kafkaConfig.valueNamingStrategy, isKey = false, kafkaConfig.valueSchemaId)

    df.select(to_avro(allColumns, valueAvroConfig) as 'value)
  }

  private[pramen] def getKeyValueDataFrame(df: DataFrame): DataFrame = {
    val keyColumns = struct(kafkaConfig.keyColumns.map(c => df(c)): _*)
    val allColumns = struct(df.columns.map(c => df(c)): _*)

    val keyAvroConfig = getAvroConfig(keyColumns, kafkaConfig.keyNamingStrategy.get, isKey = true, kafkaConfig.keySchemaId)
    val valueAvroConfig = getAvroConfig(allColumns, kafkaConfig.valueNamingStrategy, isKey = false, kafkaConfig.valueSchemaId)

    df.select(
      to_avro(keyColumns, keyAvroConfig) as 'key,
      to_avro(allColumns, valueAvroConfig) as 'value)
  }

  private[pramen] def registerSchema(columns: Column,
                                      schemaRegistryClientConfig: Map[String, String],
                                      namingStrategy: NamingStrategy,
                                      isKey: Boolean): Int = {
    // generate schema
    val expression = columns.expr
    val schema = fixNullableFields(
      convertSparkToAvroSchema(expression.dataType)
    )

    // register schema
    val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)
    val subject = namingStrategy.getSubject(topicName, isKey)
    val id = schemaManager.register(subject, schema)

    log.info(s"Schema for subject '$subject' is registered with id = $id")

    id
  }

  private[pramen] def getAvroConfig(columns: Column,
                                     namingStrategy: NamingStrategy,
                                     isKey: Boolean,
                                     schemaIdOpt: Option[Int]): ToAvroConfig = {
    val schemaRegistryClientConfig = Map(
      AbrisConfig.SCHEMA_REGISTRY_URL -> kafkaConfig.schemaRegistryUrl
    ) ++ kafkaConfig.schemaRegistryExtraOptions

    ConfigUtils.logExtraOptions("Schema registry options", schemaRegistryClientConfig, Set("basic.auth.user.info"))

    val schemaId = schemaIdOpt match {
      case Some(id) => id
      case None => registerSchema(columns, schemaRegistryClientConfig, namingStrategy, isKey)
    }

    AbrisConfig.toConfluentAvro
      .downloadSchemaById(schemaId)
      .usingSchemaRegistry(schemaRegistryClientConfig)
  }
}

object TableWriterKafka {
  private val log = LoggerFactory.getLogger(this.getClass)

  def apply(topicName: String, conf: Config)(implicit spark: SparkSession): TableWriterKafka = {
    val kafkaConfig = KafkaWriterConfig.fromConfig(conf)

    new TableWriterKafka(topicName, kafkaConfig, getExtraOptions(conf))
  }

  private[pramen] def getExtraOptions(conf: Config): Map[String, String] = {
    val extraOptions: Map[String, String] =
      ConfigUtils.getExtraOptions(conf, KafkaWriterConfig.KAFKA_WRITER_PREFIX + ".option")

    val parsedExtraOptions = extraOptions.map { case (k, v) => s"$k = $v" }
    log.debug(s"Extra options: \n${parsedExtraOptions.mkString("\n")}")
    extraOptions
  }
}
