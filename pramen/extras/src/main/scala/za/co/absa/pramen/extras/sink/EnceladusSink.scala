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
import za.co.absa.pramen.api.{ExternalChannelFactory, MetastoreReader, Sink, SinkResult}
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.hive.HiveQueryTemplates.TEMPLATES_DEFAULT_PREFIX
import za.co.absa.pramen.core.utils.hive._
import za.co.absa.pramen.extras.infofile.InfoFileGeneration
import za.co.absa.pramen.extras.notification.EcsNotificationTarget
import za.co.absa.pramen.extras.query.{QueryExecutor => EnceladusQueryExecutor, QueryExecutorSpark => EnceladusQueryExecutorSpark}
import za.co.absa.pramen.extras.sink.EnceladusConfig.DEFAULT_PUBLISH_PARTITION_TEMPLATE
import za.co.absa.pramen.extras.utils.{FsUtils, MainRunner, PartitionUtils}

import java.time.{Instant, LocalDate}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

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
  *    # Optional S3 version buckets cleanup via a special REST API
  *    cleanup.api.url = "https://hostname/api/path"
  *    cleanup.api.key = "aabbccdd"
  *    cleanup.api.trust.all.ssl.certificates = false
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
  *        output.info.version = 1
  *
  *        ## Set this up only if you want to run Standardization and Conformance
  *        output.dataset.name = "my_dataset"
  *        output.dataset.version = 1
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
                    enceladusConfig: EnceladusConfig,
                    hiveHelper: HiveHelper) extends Sink {

  import za.co.absa.pramen.extras.sink.EnceladusSink._

  private val log = LoggerFactory.getLogger(this.getClass)

  override val config: Config = sinkConfig

  override def connect(): Unit = {}

  override def close(): Unit = {}

  override def send(df: DataFrame,
                    tableName: String,
                    metastore: MetastoreReader,
                    infoDate: LocalDate,
                    options: Map[String, String])(implicit spark: SparkSession): SinkResult = {
    implicit val queryExecutor: EnceladusQueryExecutor = new EnceladusQueryExecutorSpark(spark)

    val jobStart = Instant.now()
    val basePath = getBasePath(tableName, options)
    val infoVersion = getInfoVersion(tableName, infoDate, basePath, options)
    val outputPartitionPath = getOutputPartitionPath(basePath, infoDate, infoVersion)

    val count = df.count()

    if (count > 0 || enceladusConfig.saveEmpty) {
      writeToRawFolder(df, count, outputPartitionPath)
      generateInfoFile(count, count, outputPartitionPath, infoDate, jobStart)
    } else {
      val outputPathStr = outputPartitionPath.toUri.toString
      log.info(s"Nothing to save to the Enceladus raw folder: $outputPathStr")
    }

    runEnceladusIfNeeded(tableName, infoDate, infoVersion, basePath, options)

    cleanUpS3Versions(basePath, infoDate, infoVersion, options)

    val hiveTable = options.get(HIVE_TABLE_KEY).map(getHiveTableFullName)

    val hiveTablesExposed = hiveTable match {
      case Some(table) => Seq(table)
      case None => Nil
    }

    SinkResult(count, Nil, hiveTablesExposed)
  }

  private[extras] def getInfoVersion(metaTable: String,
                                     infoDate: LocalDate,
                                     rawBasePath: Path,
                                     options: Map[String, String])
                                    (implicit spark: SparkSession, queryExecutor: EnceladusQueryExecutor): Int = {
    val publishBasePath = options.get(PUBLISH_BASE_PATH_KEY).map(s => new Path(s))
    val hiveTable = options.get(HIVE_TABLE_KEY).map(getHiveTableFullName)

    val versionStr = options.getOrElse(INFO_VERSION_KEY, INFO_VERSION_AUTO_VALUE)
    if (versionStr.toLowerCase() == INFO_VERSION_AUTO_VALUE) {
      autoDetectVersionNumber(metaTable, infoDate, rawBasePath, publishBasePath, hiveTable)
    } else {
      versionStr.toInt
    }
  }

  private[extras] def getBasePath(tableName: String,
                                  options: Map[String, String]): Path = {
    if (!options.contains(OUTPUT_PATH_KEY)) {
      throw new IllegalArgumentException(s"$OUTPUT_PATH_KEY is not specified for Enceladus sink, table: $tableName")
    }

    new Path(options(OUTPUT_PATH_KEY))
  }

  private[extras] def getOutputPartitionPath(basePath: Path,
                                             infoDate: LocalDate,
                                             infoVersion: Int): Path = {
    val partition = PartitionUtils.unpackCustomPartitionPattern(enceladusConfig.partitionPattern, enceladusConfig.infoDateColumn, infoDate, infoVersion)
    new Path(basePath, partition)
  }

  private[extras] def getPublishPartitionPath(basePath: Path,
                                             infoDate: LocalDate,
                                             infoVersion: Int): Path = {
    val partition = PartitionUtils.unpackCustomPartitionPattern(DEFAULT_PUBLISH_PARTITION_TEMPLATE, enceladusConfig.infoDateColumn, infoDate, infoVersion)
    new Path(basePath, partition)
  }

  private[extras] def writeToRawFolder(df: DataFrame,
                                       recordCount: Long,
                                       outputPartitionPath: Path)(implicit spark: SparkSession): Unit = {
    val outputPathStr = outputPartitionPath.toUri.toString
    log.info(s"Saving $recordCount records to the Enceladus raw folder: $outputPathStr")

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, outputPathStr)
    fsUtils.createDirectoryRecursiveButLast(outputPartitionPath)

    val dfToWrite = enceladusConfig.recordsPerPartition match {
      case Some(rpp) =>
        val n = Math.max(1, Math.ceil(recordCount.toDouble / rpp)).toInt
        log.info(s"Repartitioning to $n partitions...")
        df.repartition(n)
      case None      =>
        df
    }

    try {
      dfToWrite.write
        .mode(enceladusConfig.mode)
        .format(enceladusConfig.format)
        .options(enceladusConfig.formatOptions)
        .save(outputPathStr)
    } catch {
      case NonFatal(ex) =>
        log.error(s"Unable to write to: $outputPathStr", ex)
        throw ex
    }

    if (!fsUtils.exists(outputPartitionPath))
      throw new IllegalStateException(s"Unable to write to: $outputPathStr")
  }

  private[extras] def generateInfoFile(sourceCount: Long,
                                       recordCount: Long,
                                       outputPartitionPath: Path,
                                       infoDate: LocalDate,
                                       jobStart: Instant
                                      )(implicit spark: SparkSession): Unit = {
    if (enceladusConfig.generateInfoFile) {
      InfoFileGeneration.generateInfoFile(enceladusConfig.pramenVersion,
        enceladusConfig.timezoneId,
        sourceCount,
        recordCount,
        None,
        outputPartitionPath,
        infoDate,
        jobStart,
        jobStart,
        None)(spark, sinkConfig)
    }
  }

  private[extras] def runEnceladusIfNeeded(tableName: String,
                                           infoDate: LocalDate,
                                           infoVersion: Int,
                                           basePath: Path,
                                           options: Map[String, String])
                                          (implicit spark: SparkSession): Unit = {
    if (options.contains(DATASET_NAME_KEY) && options.contains(DATASET_VERSION_KEY)) {
      val datasetName = options(DATASET_NAME_KEY)
      val datasetVersion = options(DATASET_VERSION_KEY).toInt

      val publishBase = getPublishBase(basePath, options)
      val outputPublishPath = getPublishPartitionPath(new Path(publishBase), infoDate, infoVersion)

      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, publishBase)

      val stdPath = new Path(s"/tmp/standardization1_output/standardized-$datasetName-$datasetVersion-$infoDate-$infoVersion")

      log.info(s"Checking existence of $stdPath")
      if (fsUtils.exists(stdPath)) {
        log.warn(s"Removing old standardization data at $stdPath")
        fsUtils.deleteDirectoryRecursively(stdPath)
      }

      log.info(s"Checking existence of $outputPublishPath")

      if (fsUtils.exists(outputPublishPath) && outputPublishPath.toString.contains("/enceladus_info_version")) {
        log.warn(s"Removing old published data at $outputPublishPath")
        fsUtils.deleteDirectoryRecursively(outputPublishPath)
      }

      runEnceladus(
        tableName,
        datasetName,
        datasetVersion,
        infoDate,
        infoVersion,
        basePath
      )
      if (options.contains(HIVE_TABLE_KEY)) {
        Try {
          updateTable(options(HIVE_TABLE_KEY), publishBase, infoDate, infoVersion)
        } match {
          case Success(_)  =>
            log.info(s"Hive table '${options(HIVE_TABLE_KEY)}' was updated successfully.")
          case Failure(ex) =>
            throw new IllegalStateException(s"Failed to create or update Hive table '${options(HIVE_TABLE_KEY)}'.", ex)
        }
      }
    } else {
      log.info(s"Enceladus dataset name and/or version are not specified, skipping the Enceladus execution for $tableName.")
    }
  }

  private[extras] def getPublishBase(basePath: Path, options: Map[String, String]): String = {
    if (options.contains(PUBLISH_BASE_PATH_KEY)) {
      options(PUBLISH_BASE_PATH_KEY)
    } else {
      basePath.toString.replace("/raw/", "/publish/")
    }
  }

  private[extras] def runEnceladus(tableName: String,
                                   datasetName: String,
                                   datasetVersion: Int,
                                   infoDate: LocalDate,
                                   infoVersion: Int,
                                   basePath: Path): Unit = {
    val cmdArgs = applyCommandLineTemplate(
      enceladusConfig.enceladusCmdLineTemplate,
      datasetName,
      datasetVersion,
      infoDate,
      infoVersion,
      basePath)

    try {
      MainRunner.runMain(enceladusConfig.enceladusMainClass, cmdArgs)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Enceladus execution failed for $tableName.", ex)
    }
  }

  private[extras] def applyCommandLineTemplate(template: String,
                                               datasetName: String,
                                               datasetVersion: Int,
                                               infoDate: LocalDate,
                                               infoVersion: Int,
                                               basePath: Path): Array[String] = {
    template
      .replaceAll("@datasetName", datasetName)
      .replaceAll("@datasetVersion", datasetVersion.toString)
      .replaceAll("@infoDate", infoDate.toString)
      .replaceAll("@infoVersion", infoVersion.toString)
      .replaceAll("@rawPath", basePath.toString)
      .replaceAll("@rawFormat", enceladusConfig.format)
      .split(' ')
  }

  private[extras] def cleanUpS3Versions(rawBasePath: Path,
                                        infoDate: LocalDate,
                                        infoVersion: Int,
                                        options: Map[String, String]): Unit = {
    val publishBase = new Path(getPublishBase(rawBasePath, options))
    val pathStr = rawBasePath.toString
    if (!pathStr.toLowerCase.startsWith("s3://") && !pathStr.toLowerCase.startsWith("s3a://")) {
      log.info(s"The base bath ($rawBasePath) is not on S3. S3 versions cleanup won't be done.")
      return
    }

    if (!sinkConfig.hasPath(CLEANUP_API_URL_KEY) ||
      !sinkConfig.hasPath(CLEANUP_API_KEY_KEY)) {
      log.warn(s"Enceladus sink options: $CLEANUP_API_URL_KEY and $CLEANUP_API_KEY_KEY are not defined. S3 versions cleanup won't be done.")
      return
    }

    val apiUrl = sinkConfig.getString(CLEANUP_API_URL_KEY)
    val apiKey = sinkConfig.getString(CLEANUP_API_KEY_KEY)

    val rawPartitionPath = getOutputPartitionPath(rawBasePath, infoDate, infoVersion)
    val publishPartitionPath = getPublishPartitionPath(publishBase, infoDate, infoVersion)

    val trustAllSslCerts = sinkConfig.hasPath(CLEANUP_API_TRUST_SSL_KEY) && sinkConfig.getBoolean(CLEANUP_API_TRUST_SSL_KEY)
    val httpClient = EcsNotificationTarget.getHttpClient(trustAllSslCerts)

    try {
      EcsNotificationTarget.cleanUpS3VersionsForPath(rawPartitionPath, apiUrl, apiKey, httpClient)
      EcsNotificationTarget.cleanUpS3VersionsForPath(publishPartitionPath, apiUrl, apiKey, httpClient)
    } finally {
      httpClient.close()
    }
  }

  private[extras] def updateTable(hiveTable: String, publishBase: String, infoDate: LocalDate, infoVersion: Int)(implicit spark: SparkSession): Unit = {
    if (hiveHelper.doesTableExist(enceladusConfig.hiveDatabase, hiveTable)) {
      if (enceladusConfig.preferAddPartition) {
        val location = s"$publishBase/enceladus_info_date=$infoDate/enceladus_info_version=$infoVersion"
        log.info(s"Table '${getHiveTableFullName(hiveTable)}' exists. Adding new partition: '$location'...")
        hiveHelper.addPartition(enceladusConfig.hiveDatabase, hiveTable, Seq("enceladus_info_date", "enceladus_info_version"), Seq(infoDate.toString, infoVersion.toString), location)
      } else {
        log.info(s"Table '${getHiveTableFullName(hiveTable)}' exists. Repairing partitions...")
        hiveHelper.repairHiveTable(enceladusConfig.hiveDatabase, hiveTable, HiveFormat.Parquet)
      }
    } else {
      log.info(s"Table '${getHiveTableFullName(hiveTable)}' does not exist. Creating...")
      val df = spark.read.option("mergeSchema", "true").parquet(publishBase)

      val schema = df.schema

      hiveHelper.createOrUpdateHiveTable(publishBase, HiveFormat.Parquet, schema, Seq("enceladus_info_date", "enceladus_info_version"), enceladusConfig.hiveDatabase, hiveTable)
    }
  }

  private[extras] def getHiveTableFullName(hiveTable: String): String = {
    enceladusConfig.hiveDatabase match {
      case Some(db) => s"$db.$hiveTable"
      case None     => s"$hiveTable"
    }
  }

  private[extras] def autoDetectVersionNumber(metaTable: String,
                                              infoDate: LocalDate,
                                              rawBasePath: Path,
                                              publishBasePath: Option[Path],
                                              hiveTable: Option[String])
                                             (implicit spark: SparkSession, queryExecutor: EnceladusQueryExecutor): Int = {
    val enceladusUtils = new EnceladusUtils(enceladusConfig.partitionPattern,
      enceladusConfig.publishPartitionPattern,
      enceladusConfig.infoDateColumn)

    val versionTry = enceladusUtils.getNextEnceladusVersion(infoDate,
      rawBasePath,
      publishBasePath,
      hiveTable
    )

    versionTry match {
      case Success(version) =>
        log.warn(s"Autodetected next info version for $metaTable: $version.")
        version
      case Failure(ex)      =>
        throw new IllegalStateException(s"Could not autodetect Enceladus version for $metaTable.", ex)
    }
  }
}

object EnceladusSink extends ExternalChannelFactory[EnceladusSink] {
  val OUTPUT_PATH_KEY = "path"
  val INFO_VERSION_KEY = "info.version"
  val DATASET_NAME_KEY = "dataset.name"
  val DATASET_VERSION_KEY = "dataset.version"
  val HIVE_TABLE_KEY = "hive.table"
  val HIVE_ALWAYS_ESCAPE_COLUMN_NAMES = "hive.escape.column.names"
  val PUBLISH_BASE_PATH_KEY = "publish.base.path"
  val CLEANUP_API_URL_KEY = "cleanup.api.url"
  val CLEANUP_API_KEY_KEY = "cleanup.api.key"
  val CLEANUP_API_TRUST_SSL_KEY = "cleanup.api.trust.all.ssl.certificates"

  val INFO_VERSION_AUTO_VALUE = "auto"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): EnceladusSink = {
    val enceladusConfig = EnceladusConfig.fromConfig(conf)
    val alwaysEscapeColumnNames = ConfigUtils.getOptionBoolean(conf, HIVE_ALWAYS_ESCAPE_COLUMN_NAMES).getOrElse(true)

    val hiveTemplates = HiveQueryTemplates.fromConfig(ConfigUtils.getOptionConfig(conf, TEMPLATES_DEFAULT_PREFIX))
    val queryExecutor = QueryExecutorSpark(spark)

    val hiveHelper = new HiveHelperSql(queryExecutor, hiveTemplates, alwaysEscapeColumnNames)

    new EnceladusSink(conf, enceladusConfig, hiveHelper)
  }
}
