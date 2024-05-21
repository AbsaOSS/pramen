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

package za.co.absa.pramen.core.sink

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{ExternalChannelFactory, MetastoreReader, Sink, SinkResult}
import za.co.absa.pramen.core.config.Keys.KEYS_TO_REDACT
import za.co.absa.pramen.core.sink.SparkSinkFormat.{ConnectionFormat, PathFormat, TableFormat}
import za.co.absa.pramen.core.utils.{AlgorithmUtils, ConfigUtils}

import java.time.LocalDate

/**
  * This sink allows writing data using Spark, similarly as you would do using 'df.write.format(...).save(...)'.
  *
  * In order to use the sink you need to define sink parameters.
  *
  * Example sink definition:
  * {{{
  *  {
  *    # Define a name to reference from the pipeline:
  *    name = "spark_sink"
  *    factory.class = "za.co.absa.pramen.core.sink.SparkSink"
  *
  *    # Output format. Can be: csv, parquet, json, delta, etc (anything supported by Spark). Default: parquet
  *    format = "parquet"
  *
  *    # Save mode. Can be overwrite, append, ignore, errorifexists. Default: errorifexists
  *    mode = "overwrite"
  *
  *    ## Only one of these following two options should be specified
  *    # Optionally repartition the dataframe according to the specified number of partitions
  *    number.of.partitions = 10
  *    # Optionally repartition te dataframe according to the number of records per partition
  *    records.per.partition = 1000000
  *
  *    # The number of attempts to make against the target
  *    retries = 5
  *
  *    # If true (default), the data will be saved even if it does not contain any records. If false, the saving will be skipped
  *    save.empty = true
  *
  *    # If non-empty, the data will be partitioned by the specified columns at the output path. Default: []
  *    partition.by = [ pramen_info_date ]
  *
  *    # These are additional option passed to the writer as 'df.write(...).options(...)
  *    option {
  *      compression = "gzip"
  *    }
  *  }
  * }}}
  *
  * Here is an example of a sink definition in a pipeline. As for any other operation you can specify
  * dependencies, transformations, filters and columns to select.
  *
  * {{{
  *  {
  *    name = "Spark sink"
  *    type = "sink"
  *    sink = "spark_sink"
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
  *        input.metastore.table = metastore_table
  *        output.path = "/datalake/base/path"
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
  *
  *        # This overrides options of the sink
  *        sink {
  *          mode = "append"
  *
  *          # These are additional options passed to the writer as 'df.write(...).options(...)'
  *          option {
  *             compression = "snappy"
  *          }
  *        }
  *      }
  *    ]
  *  }
  * }}}
  *
  */
class SparkSink(format: String,
                formatOptions: Map[String, String],
                mode: String,
                partitionBy: Seq[String],
                numberOfPartitions: Option[Int],
                recordsPerPartition: Option[Long],
                saveEmpty: Boolean,
                retries: Int,
                sinkConfig: Config) extends Sink {

  import za.co.absa.pramen.core.sink.SparkSink._

  override val config: Config = sinkConfig

  override def connect(): Unit = {}

  override def close(): Unit = {}

  override def send(df: DataFrame,
                    tableName: String,
                    metastore: MetastoreReader,
                    infoDate: LocalDate,
                    options: Map[String, String])
                   (implicit spark: SparkSession): SinkResult = {
    val outputFormat = getOutputFormat(tableName, options)
    val recordCount = df.count()

    if (recordCount > 0 || saveEmpty) {
      log.info(s"Saving $recordCount records to ${outputFormat.toString}...")
      log.info(s"Mode: $mode")

      if (formatOptions.nonEmpty) {
        log.info(s"Options passed for '$format':")
        ConfigUtils.renderExtraOptions(formatOptions, KEYS_TO_REDACT)(log.info)
      }

      val dfToWrite = applyRepartitioning(df, recordCount, tableName)
      writeData(dfToWrite, outputFormat)
    } else {
      log.info(s"Nothing to save to ${outputFormat.toString}")
    }

    SinkResult(recordCount)
  }

  private[core] def writeData(df: DataFrame, outputFormat: SparkSinkFormat): Unit = {
    val saver = df
      .write
      .partitionBy(partitionBy: _*)
      .format(format)
      .mode(mode)
      .options(formatOptions)

    AlgorithmUtils.actionWithRetry(retries, log) {
      outputFormat match {
        case PathFormat(path) =>
          saver.save(path.toUri.toString)
        case TableFormat(table) =>
          saver.saveAsTable(table)
        case _: ConnectionFormat =>
          saver.save()
      }
    }
  }

  private[core] def applyRepartitioning(df: DataFrame, recordCount: Long, tableName: String): DataFrame = {
    (numberOfPartitions, recordsPerPartition) match {
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException(
          s"Both $NUMBER_OF_PARTITIONS_KEY and $RECORDS_PER_PARTITION_KEY are specified for Spark sink," +
            s"table: $tableName. Please specify only one of those options")
      case (Some(nop), None) =>
        log.info(s"Repartitioning to $nop partitions")
        df.repartition(nop)
      case (None, Some(rpp)) =>
        val n = Math.max(1, Math.ceil(recordCount.toDouble / rpp)).toInt
        log.info(s"Repartitioning to $n partitions")
        df.repartition(n)
      case (None, None) =>
        df
    }
  }

  private[core] def getOutputFormat(tableName: String, options: Map[String, String]): SparkSinkFormat = {
    if (FILE_FORMATS.contains(format.toLowerCase) && !options.contains(OUTPUT_PATH_KEY)) {
      throw new IllegalArgumentException(s"$OUTPUT_PATH_KEY is not specified for Spark sink, table: $tableName")
    }

    if (options.contains(OUTPUT_PATH_KEY)) {
      PathFormat(new Path(options(OUTPUT_PATH_KEY)))
    } else if (options.contains(OUTPUT_TABLE_KEY)) {
      TableFormat(options(OUTPUT_TABLE_KEY))
    } else {
      ConnectionFormat(format)
    }
  }
}

object SparkSink extends ExternalChannelFactory[SparkSink] {
  private val log = LoggerFactory.getLogger(this.getClass)

  val OUTPUT_PATH_KEY = "path"
  val OUTPUT_TABLE_KEY = "table"

  val FORMAT_KEY = "format"
  val MODE_KEY = "mode"
  val PARTITION_BY_KEY = "partition.by"
  val NUMBER_OF_PARTITIONS_KEY = "number.of.partitions"
  val RECORDS_PER_PARTITION_KEY = "records.per.partition"
  val SAVE_EMPTY_KEY = "save.empty"
  val RETRIES_KEY = "retries"

  val DEFAULT_FORMAT = "parquet"
  val DEFAULT_MODE = "errorifexists"
  val DEFAULT_SAVE_EMPTY = true

  val FILE_FORMATS: Seq[String] = Seq("csv", "json", "parquet", "avro", "orc", "xml")

  override def apply(conf: Config, parentPath: String, spark: SparkSession): SparkSink = {
    new SparkSink(
      ConfigUtils.getOptionString(conf, FORMAT_KEY).getOrElse(DEFAULT_FORMAT),
      ConfigUtils.getExtraOptions(conf, "option"),
      ConfigUtils.getOptionString(conf, MODE_KEY).getOrElse(DEFAULT_MODE),
      ConfigUtils.getOptListStrings(conf, PARTITION_BY_KEY),
      ConfigUtils.getOptionInt(conf, NUMBER_OF_PARTITIONS_KEY),
      ConfigUtils.getOptionLong(conf, RECORDS_PER_PARTITION_KEY),
      ConfigUtils.getOptionBoolean(conf, SAVE_EMPTY_KEY).getOrElse(DEFAULT_SAVE_EMPTY),
      ConfigUtils.getOptionInt(conf, RETRIES_KEY).getOrElse(1),
      conf
    )
  }
}
