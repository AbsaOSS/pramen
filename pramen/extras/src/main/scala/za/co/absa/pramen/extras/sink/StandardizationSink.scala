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
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{ExternalChannelFactory, MetastoreReader, Sink}
import za.co.absa.pramen.extras.infofile.InfoFileGeneration
import za.co.absa.pramen.extras.query.{QueryExecutor, QueryExecutorSpark}
import za.co.absa.pramen.extras.utils.{FsUtils, MainRunner, PartitionUtils}

import java.time.{Instant, LocalDate}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * This sink allows sending data to the raw and publish folder of a data lake for further processing.
  *
  * In order to use the sink you need to define sink parameters.
  *
  * Example sink definition:
  * {{{
  *  {
  *    # Define a name to reference from the pipeline:
  *    name = "my_standardization_sink"
  *    factory.class = "za.co.absa.pramen.extras.sink.StandardizationSink"
  *
  *    # (optional) Raw partition pattern. Default: {year}/{month}/{day}/v{version}
  *    raw.partition.pattern = "{year}/{month}/{day}/v{version}"
  *
  *    # (optional) Publish partition pattern. Default: {year}/{month}/{day}/v{version}
  *    publish.partition.pattern = "enceladus_info_date={year}-{month}-{day}/enceladus_info_version={version}"
  *
  *    # (optional) Repartition te dataframe according to the number of records per partition
  *    records.per.partition = 1000000
  *
  *    # (optional) A hive database to use for creating/repairing Hive tables
  *    hive.database = mydb
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
  *    name = "Standardization sink"
  *    type = "sink"
  *    sink = "my_standardization_sink"
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
  *        metastore.table = my_table1
  *        # or
  *        # input.path = ...
  *        # job.metastore.table = my_table1
  *        #
  *
  *        output.raw.base.path = "/datalake/base/path/raw/my_table1"
  *        output.publish.base.path = "/datalake/base/path/publish/my_table1"
  *
  *        # All following settings are OPTIONAL
  *
  *        # Info version (default = 1)
  *        output.info.version = 1
  *
  *        # Hive table to create/repair
  *        output.hive.table = "my_hive_table"
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
class StandardizationSink(sinkConfig: Config,
                          publishConfig: StandardizationConfig) extends Sink {

  import za.co.absa.pramen.extras.sink.StandardizationSink._

  private val log = LoggerFactory.getLogger(this.getClass)

  override val config: Config = sinkConfig

  override def connect(): Unit = {}

  override def close(): Unit = {}

  override def send(df: DataFrame,
                    tableName: String,
                    metastore: MetastoreReader,
                    infoDate: LocalDate,
                    options: Map[String, String])(implicit spark: SparkSession): Long = {
    implicit val queryExecutor: QueryExecutor = new QueryExecutorSpark(spark)

    val jobStart = Instant.now()
    val rawBasePath = getBasePath(tableName, RAW_BASE_PATH_KEY, options)
    val publishBasePath = getBasePath(tableName, RAW_BASE_PATH_KEY, options)
    val infoVersion = getInfoVersion(options)

    val outputRawPartitionPath = getPartitionPath(rawBasePath, infoDate, infoVersion)
    val outputPublishPartitionPath = getPartitionPath(publishBasePath, infoDate, infoVersion)

    val sourceCount = df.count()

    val dfToWrite = repartitionIfNeeded(df, sourceCount)

    writeToRawFolder(dfToWrite, sourceCount, outputRawPartitionPath)

    val rawCount = spark.read.json(outputRawPartitionPath.toUri.toString).count
    generateInfoFile(sourceCount, rawCount, None, outputRawPartitionPath, infoDate, jobStart, None)

    val publishStart = Instant.now()

    writeToPublishFolder(dfToWrite, sourceCount, outputPublishPartitionPath)
    val publishCount = spark.read.parquet(outputPublishPartitionPath.toUri.toString).count
    generateInfoFile(sourceCount, rawCount, Option(publishCount), outputPublishPartitionPath, infoDate, jobStart, Some(publishStart))

    sourceCount
  }

  private[extras] def getInfoVersion(options: Map[String, String]): Int = {
    val versionStr = options.getOrElse(INFO_VERSION_KEY, "1")
    versionStr.toInt
  }

  private[extras] def getBasePath(tableName: String,
                                  configKey: String,
                                  options: Map[String, String]): Path = {
    if (!options.contains(configKey)) {
      throw new IllegalArgumentException(s"$configKey is not specified for Enceladus sink, table: $tableName")
    }

    new Path(options(configKey))
  }

  private[extras] def getPartitionPath(basePath: Path,
                                       infoDate: LocalDate,
                                       infoVersion: Int): Path = {
    val partition = PartitionUtils.unpackCustomPartitionPattern(publishConfig.rawPartitionPattern, StandardizationConfig.INFO_DATE_COLUMN, infoDate, infoVersion)
    new Path(basePath, partition)
  }

  private[extras] def repartitionIfNeeded(df: DataFrame, recordCount: Long): DataFrame = {
    publishConfig.recordsPerPartition match {
      case Some(rpp) =>
        val n = Math.max(1, Math.ceil(recordCount.toDouble / rpp)).toInt
        log.info(s"Repartitioning to $n partitions...")
        df.repartition(n)
      case None      =>
        df
    }
  }

  private[extras] def writeToRawFolder(df: DataFrame,
                                       recordCount: Long,
                                       outputPartitionPath: Path)(implicit spark: SparkSession): Unit = {
    val outputPathStr = outputPartitionPath.toUri.toString
    log.info(s"Saving $recordCount records to the Enceladus raw folder: $outputPathStr")

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, outputPathStr)
    fsUtils.createDirectoryRecursiveButLast(outputPartitionPath)

    df.write
      .mode(SaveMode.Overwrite)
      .json(outputPathStr)
  }

  private[extras] def writeToPublishFolder(df: DataFrame,
                                           recordCount: Long,
                                           outputPartitionPath: Path)(implicit spark: SparkSession): Unit = {
    val outputPathStr = outputPartitionPath.toUri.toString
    log.info(s"Saving $recordCount records to the Enceladus publish folder: $outputPathStr")

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, outputPathStr)
    fsUtils.createDirectoryRecursiveButLast(outputPartitionPath)

    df.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPathStr)
  }

  private[extras] def generateInfoFile(sourceCount: Long,
                                       rawCount: Long,
                                       publishCount: Option[Long],
                                       outputPartitionPath: Path,
                                       infoDate: LocalDate,
                                       jobStart: Instant,
                                       publishStart: Option[Instant]
                                      )(implicit spark: SparkSession): Unit = {
    if (publishConfig.generateInfoFile) {
      InfoFileGeneration.generateInfoFile(publishConfig.pramenVersion,
        publishConfig.timezoneId,
        sourceCount,
        rawCount,
        publishCount,
        outputPartitionPath,
        infoDate,
        jobStart,
        jobStart,
        publishStart)(spark, sinkConfig)
    }
  }

  private[extras] def getHiveTableFullName(hiveTable: String): String = {
    publishConfig.hiveDatabase match {
      case Some(db) => s"$db.$hiveTable"
      case None     => s"$hiveTable"
    }
  }

  private[extras] def getHiveRepairQuery(hiveFullTableName: String): String = {
    s"MSCK REPAIR TABLE $hiveFullTableName"
  }
}

object StandardizationSink extends ExternalChannelFactory[EnceladusSink] {
  val RAW_BASE_PATH_KEY = "raw.base.path"
  val PUBLISH_BASE_PATH_KEY = "publish.base.path"
  val INFO_VERSION_KEY = "info.version"
  val DATASET_NAME_KEY = "dataset.name"
  val DATASET_VERSION_KEY = "dataset.version"
  val HIVE_TABLE_KEY = "hive.table"

  val INFO_VERSION_AUTO_VALUE = "auto"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): EnceladusSink = {
    val enceladusConfig = EnceladusConfig.fromConfig(conf)
    new EnceladusSink(conf, enceladusConfig)
  }
}
