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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{ExternalChannelFactory, MetastoreReader, Sink}
import za.co.absa.pramen.extras.infofile.InfoFileGeneration
import za.co.absa.pramen.extras.utils.{FsUtils, PartitionUtils}

import java.time.{Instant, LocalDate}

/**
  * This sink allows sending data to the raw folder of a data lake for further processing by Enceladus+Menas:
  * https://github.com/AbsaOSS/enceladus
  *
  * In order to use the sink you need to define sink parameters.
  *
  * Example sink definition:
  * {{{
  *  {
  *    # Define a name to reference from the pipeline:
  *    name = "enceladus_raw"
  *    factory.class = "za.co.absa.pramen.extras.sink.EnceladusSink"
  *
  *    # Output format. Can be: csv, parquet, json, delta, etc (anything supported by Spark). Default: parquet
  *    format = "csv"
  *
  *    # Save mode. Can be overwrite, append, ignore, errorifexists. Default: errorifexists
  *    mode = "overwrite"
  *
  *    # Information date column, default: enceladus_info_date
  *    info.date.column = "enceladus_info_date"
  *
  *    # Partition pattern. Default: {year}/{month}/{day}/v{version}
  *    partition.pattern = "{year}/{month}/{day}/v{version}"
  *
  *    # If true (default), the data will be saved even if it does not contain any records. If false, the saving will be skipped
  *    save.empty = true
  *
  *    # Optionally repartition te dataframe according to the number of records per partition
  *    records.per.partition = 1000000
  *
  *    # Output format options
  *    option {
  *      sep = "|"
  *      quoteAll = "false"
  *      header = "false"
  *    }
  *
  *    # Info file options
  *    info.file {
  *      generate = true
  *
  *      source.application = "Unspecified"
  *      country = "Africa"
  *      history.type = "Snapshot"
  *
  *      timestamp.format = "dd-MM-yyyy HH:mm:ss Z"
  *      date.format = "yyyy-MM-dd"
  *    }
  *  }
  * }}}
  *
  * Here is an example of a sink definition in a pipeline. As for any other operation you can specify
  * dependencies, transformations, filters and columns to select.
  *
  * {{{
  *  {
  *    name = "Enceladus sink"
  *    type = "sink"
  *    sink = "enceladus_raw"
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
  *        output.path = "/datalake/base/path"
  *
  *        # All following settings are OPTIONAL
  *
  *        # Info version (default = 1)
  *        info.version = 1
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
class EnceladusSink(sinkConfig: Config,
                    enceladusConfig: EnceladusConfig) extends Sink {

  import za.co.absa.pramen.extras.sink.EnceladusSink._

  private val log = LoggerFactory.getLogger(this.getClass)

  override val config: Config = sinkConfig

  override def connect(): Unit = {}

  override def close(): Unit = {}

  override def send(df: DataFrame,
                    tableName: String,
                    metastore: MetastoreReader,
                    infoDate: LocalDate,
                    options: Map[String, String])(implicit spark: SparkSession): Long = {
    val jobStart = Instant.now()

    val infoVersion = getInfoVersion(options)
    val outputPath = getOutputPath(tableName, infoDate, infoVersion, options)

    val count = df.count()

    if (count > 0 || enceladusConfig.saveEmpty) {
      writeToRawFolder(df, count, outputPath)
      generateInfoFile(df, count, outputPath, infoDate, jobStart)
    } else {
      val outputPathStr = outputPath.toUri.toString
      log.info(s"Nothing to save to the Enceladus raw folder: $outputPathStr")
    }

    count
  }

  private[extras] def getInfoVersion(options: Map[String, String]): Int = {
    // ToDo This can be improver by automatically determining the version based on the existing folders.
    options.getOrElse(INFO_VERSION_KEY, "1").toInt
  }

  private[extras] def getOutputPath(tableName: String,
                                    infoDate: LocalDate,
                                    infoVersion: Int,
                                    options: Map[String, String]): Path = {
    if (!options.contains(OUTPUT_PATH_KEY)) {
      throw new IllegalArgumentException(s"$OUTPUT_PATH_KEY is not specified for Enceladus sink, table: $tableName")
    }

    val basePath = new Path(options(OUTPUT_PATH_KEY))

    val partition = PartitionUtils.unpackCustomPartitionPattern(enceladusConfig.partitionPattern, enceladusConfig.infoDateColumn, infoDate, infoVersion)
    new Path(basePath, partition)
  }

  private[extras] def writeToRawFolder(df: DataFrame,
                                       recordCount: Long,
                                       outputPath: Path)(implicit spark: SparkSession): Unit = {
    val outputPathStr = outputPath.toUri.toString
    log.info(s"Saving $recordCount records to the Enceladus raw folder: $outputPathStr")

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, outputPathStr)
    fsUtils.createDirectoryRecursiveButLast(outputPath)

    val dfToWrite = enceladusConfig.recordsPerPartition match {
      case Some(rpp) =>
        val n = Math.max(1, Math.ceil(recordCount.toDouble / rpp)).toInt
        log.info(s"Repartitioning to $n partitions...")
        df.repartition(n)
      case None      =>
        df
    }

    dfToWrite.write
      .mode(enceladusConfig.mode)
      .format(enceladusConfig.format)
      .options(enceladusConfig.formatOptions)
      .save(outputPathStr)
  }

  private[extras] def generateInfoFile(df: DataFrame,
                                       recordCount: Long,
                                       outputPath: Path,
                                       infoDate: LocalDate,
                                       jobStart: Instant
                                      )(implicit spark: SparkSession): Unit = {
    if (enceladusConfig.generateInfoFile) {
      InfoFileGeneration.generateInfoFile(enceladusConfig.pramenVersion,
        enceladusConfig.timezoneId,
        recordCount,
        df,
        outputPath,
        infoDate,
        jobStart,
        jobStart)(spark, sinkConfig)
    }
  }
}

object EnceladusSink extends ExternalChannelFactory[EnceladusSink] {
  val OUTPUT_PATH_KEY = "path"
  val INFO_VERSION_KEY = "info.version"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): EnceladusSink = {
    val enceladusConfig = EnceladusConfig.fromConfig(conf)
    new EnceladusSink(conf, enceladusConfig)
  }
}
