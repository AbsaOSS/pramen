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
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{ExternalChannelFactory, MetastoreReader, Sink, SinkResult}
import za.co.absa.pramen.core.metastore.model.HiveApi
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.Emoji.FAILURE
import za.co.absa.pramen.core.utils.JvmUtils.getShortExceptionDescription
import za.co.absa.pramen.core.utils.hive.HiveQueryTemplates.TEMPLATES_DEFAULT_PREFIX
import za.co.absa.pramen.core.utils.hive._
import za.co.absa.pramen.extras.infofile.InfoFileGeneration
import za.co.absa.pramen.extras.utils.PartitionUtils

import java.sql.Date
import java.time.{Instant, LocalDate}
import scala.util.control.NonFatal

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
  *    # Enceladus additional columns
  *    info.date.column = "enceladus_info_date"
  *    info.date.str.column = "enceladus_info_date_string"
  *    info.version.column = "enceladus_info_version"
  *
  *    hive = {
  *      # (optional) A hive database to use for creating/repairing Hive tables
  *      database = mydb
  *
  *      # For parquet:
  *      api = "sql"
  *      # For delta:
  *      # api = "spark_catalog"
  *      ignore.failures = false
  *    }
  *
  *    # (optional) Set timezone for info file fields
  *    timezone = "Africa/Johannesburg"
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
  *        # Hive path if it is different from publish path
  *        output.hive.path = "/d/e/f"
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
                          standardizationConfig: StandardizationConfig,
                          hiveHelper: HiveHelper) extends Sink {

  import za.co.absa.pramen.extras.sink.StandardizationSink._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val partitionBy = if (standardizationConfig.publishPartitionPattern.contains(standardizationConfig.infoVersionColumn)) {
    Seq(standardizationConfig.infoDateColumn, standardizationConfig.infoVersionColumn)
  } else {
    Seq(standardizationConfig.infoDateColumn)
  }

  override val config: Config = sinkConfig

  override def connect(): Unit = {}

  override def close(): Unit = {}

  override def send(df: DataFrame,
                    tableName: String,
                    metastore: MetastoreReader,
                    infoDate: LocalDate,
                    options: Map[String, String])(implicit spark: SparkSession): SinkResult = {
    val jobStart = Instant.now()

    val infoVersion = getInfoVersion(options)

    val sourceCount = df.count()

    val dfToWrite = addExtraFields(repartitionIfNeeded(df, sourceCount), infoDate, infoVersion)

    val (rawCount, rawDf) = if (options.contains(RAW_BASE_PATH_KEY)) {
      val rawBasePath = getBasePath(tableName, RAW_BASE_PATH_KEY, options)
      val outputRawPartitionPath = getParquetPartitionPath(rawBasePath, standardizationConfig.rawPartitionPattern, infoDate, infoVersion)
      val rawDfToWrite = dfToWrite.drop(partitionBy: _*)
      val rawDf = writeToRawFolder(rawDfToWrite, sourceCount, outputRawPartitionPath)

      val rawCount = rawDf.count
      generateInfoFile(sourceCount, rawCount, None, outputRawPartitionPath, infoDate, jobStart, None)
      (rawCount, rawDf)
    } else {
      (sourceCount, dfToWrite)
    }

    val publishStart = Instant.now()

    val publishBasePath = getBasePath(tableName, PUBLISH_BASE_PATH_KEY, options)
    val outputPublishPartitionPath = getParquetPartitionPath(publishBasePath, standardizationConfig.publishPartitionPattern, infoDate, infoVersion)

    val publishDf = standardizationConfig.publishFormat match {
      case HiveFormat.Parquet =>
        val publishDfToWrite = rawDf.drop(partitionBy: _*)
        writeToPublishFolderParquet(publishDfToWrite, sourceCount, outputPublishPartitionPath)
      case HiveFormat.Delta   =>
        writeToPublishFolderDelta(rawDf, sourceCount, publishBasePath, infoDate, infoVersion)
      case format             =>
        throw new IllegalArgumentException(s"Unsupported publish format: ${format.name}")
    }

    val publishCount = publishDf.count()

    generateInfoFile(sourceCount, rawCount, Option(publishCount), outputPublishPartitionPath, infoDate, jobStart, Some(publishStart))

    val (warnings, hiveTables) = if (options.contains(HIVE_TABLE_KEY)) {
      val hiveTable = options(HIVE_TABLE_KEY)
      val hivePath = options.get(HIVE_PATH_KEY)
      val fullTableName = HiveHelper.getFullTable(standardizationConfig.hiveDatabase, hiveTable)

      // Generating schema based on the latest ingested partition
      val fullSchema = addExtraFields(publishDf, infoDate, infoVersion).schema

      val hiveEffectivePath = hivePath.getOrElse(publishBasePath.toUri.toString)

      log.info(s"Updating Hive table '$fullTableName'...")
      try {
        hiveHelper.createOrUpdateHiveTable(hiveEffectivePath,
          standardizationConfig.publishFormat,
          fullSchema,
          partitionBy,
          standardizationConfig.hiveDatabase,
          hiveTable)
        (Seq.empty[String], Seq(fullTableName))
      } catch {
        case NonFatal(ex) =>
          if (standardizationConfig.hiveIgnoreFailures) {
            log.error(s"$FAILURE Unable to update Hive table '$fullTableName'. Ignoring the error.", ex)
            val msg = getShortExceptionDescription(ex)
            val warnings = Seq(s"Unable to update Hive table '$fullTableName': $msg")
            (warnings, Seq(fullTableName))
          } else {
            throw ex
          }
      }
    } else {
      log.info(s"Hive table is not configured for $tableName.")
      (Nil, Nil)
    }

    SinkResult(sourceCount, Nil, hiveTables, warnings)
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

  private[extras] def getParquetPartitionPath(basePath: Path,
                                              partitionPattern: String,
                                              infoDate: LocalDate,
                                              infoVersion: Int): Path = {
    val partition = PartitionUtils.unpackCustomPartitionPattern(partitionPattern, standardizationConfig.infoDateColumn, infoDate, infoVersion)
    new Path(basePath, partition)
  }

  private[extras] def repartitionIfNeeded(df: DataFrame, recordCount: Long): DataFrame = {
    standardizationConfig.recordsPerPartition match {
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
                                       outputPartitionPath: Path): DataFrame = {
    val outputPathStr = outputPartitionPath.toUri.toString
    log.info(s"Saving $recordCount records to the Enceladus raw folder: $outputPathStr")

    df.write
      .mode(SaveMode.Overwrite)
      .format(standardizationConfig.rawFormat)
      .save(outputPathStr)

    df.sparkSession
      .read
      .format(standardizationConfig.rawFormat)
      .load(outputPathStr)
  }

  private[extras] def writeToPublishFolderParquet(df: DataFrame,
                                                  recordCount: Long,
                                                  outputPartitionPath: Path): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession

    val outputPathStr = outputPartitionPath.toUri.toString
    log.info(s"Saving $recordCount records to the Enceladus publish parquet folder: $outputPathStr")

    df.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPathStr)

    spark.read.parquet(outputPartitionPath.toUri.toString)
  }

  private[extras] def writeToPublishFolderDelta(df: DataFrame,
                                                recordCount: Long,
                                                outputBasePath: Path,
                                                infoDate: LocalDate,
                                                infoVersion: Int): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession

    log.info(s"Saving $recordCount records to the Enceladus publish delta folder: $outputBasePath")

    // Publish layer can be partitioned by either enceladus_info_date & enceladus_info_version or just by enceladus_info_date
    val (replaceWhere, readFilter) = if (standardizationConfig.publishPartitionPattern.contains(standardizationConfig.infoVersionColumn)) {
      val replaceWhere = s"${standardizationConfig.infoDateColumn}='$infoDate' AND ${standardizationConfig.infoVersionColumn}=$infoVersion"

      val filter = col(standardizationConfig.infoDateColumn) === Date.valueOf(infoDate) && col(standardizationConfig.infoVersionColumn) === infoVersion
      (replaceWhere, filter)
    } else {
      val replaceWhere = s"${standardizationConfig.infoDateColumn}='$infoDate'"

      val filter = col(standardizationConfig.infoDateColumn) === Date.valueOf(infoDate)

      (replaceWhere, filter)
    }

    addExtraFields(df, infoDate, infoVersion)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionBy: _*)
      .option("mergeSchema", "true")
      .option("replaceWhere", replaceWhere)
      .save(outputBasePath.toString)

    spark.read
      .format("delta")
      .load(outputBasePath.toString)
      .filter(readFilter)
  }

  private[extras] def addExtraFields(df: DataFrame, infoDate: LocalDate, infoVersion: Int): DataFrame = {
    df.withColumn(standardizationConfig.infoDateStringColumn, lit(Date.valueOf(infoDate)).cast(StringType))
      .withColumn(standardizationConfig.infoDateColumn, lit(Date.valueOf(infoDate)))
      .withColumn(standardizationConfig.infoVersionColumn, lit(infoVersion))
  }

  private[extras] def generateInfoFile(sourceCount: Long,
                                       rawCount: Long,
                                       publishCount: Option[Long],
                                       outputPartitionPath: Path,
                                       infoDate: LocalDate,
                                       jobStart: Instant,
                                       publishStart: Option[Instant]
                                      )(implicit spark: SparkSession): Unit = {
    if (standardizationConfig.generateInfoFile) {
      InfoFileGeneration.generateInfoFile(standardizationConfig.pramenVersion,
        standardizationConfig.timezoneId,
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

  private[extras] def getHiveRepairQuery(hiveTable: String): String = getHiveRepairQueryForFullTable(getHiveTableFullName(hiveTable))

  private[extras] def getHiveTableFullName(hiveTable: String): String = {
    standardizationConfig.hiveDatabase match {
      case Some(db) => s"$db.$hiveTable"
      case None     => s"$hiveTable"
    }
  }

  private[extras] def getHiveRepairQueryForFullTable(hiveFullTableName: String): String = {
    s"MSCK REPAIR TABLE $hiveFullTableName"
  }
}

object StandardizationSink extends ExternalChannelFactory[StandardizationSink] {
  private val log = LoggerFactory.getLogger(this.getClass)

  val RAW_BASE_PATH_KEY = "raw.base.path"
  val PUBLISH_BASE_PATH_KEY = "publish.base.path"
  val INFO_VERSION_KEY = "info.version"
  val DATASET_NAME_KEY = "dataset.name"
  val DATASET_VERSION_KEY = "dataset.version"
  val HIVE_TABLE_KEY = "hive.table"
  val HIVE_ALWAYS_ESCAPE_COLUMN_NAMES = "hive.escape.column.names"
  val HIVE_PATH_KEY = "hive.path"
  val HIVE_API_SINK_KEY = "hive.api"

  val ENCELADUS_INFO_DATE_COLUMN = "enceladus_info_date"
  val ENCELADUS_INFO_VERSION_COLUMN = "enceladus_info_version"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): StandardizationSink = {
    val standardizationConfig = StandardizationConfig.fromConfig(conf)
    val hiveConfig = HiveQueryTemplates.fromConfig(ConfigUtils.getOptionConfig(conf, TEMPLATES_DEFAULT_PREFIX))
    val alwaysEscapeColumnNames = ConfigUtils.getOptionBoolean(conf, HIVE_ALWAYS_ESCAPE_COLUMN_NAMES).getOrElse(true)

    val hiveApi = if (conf.hasPath(HIVE_API_SINK_KEY))
      HiveApi.fromString(conf.getString(HIVE_API_SINK_KEY))
    else
      HiveApi.Sql

    val hiveHelper = hiveApi match {
      case HiveApi.Sql          =>
        val queryExecutor = standardizationConfig.hiveJdbcConfig match {
          case Some(hiveJdbcConfig) =>
            log.info(s"Using Hive SQL API by connecting to the Hive metastore via JDBC.")
            QueryExecutorJdbc.fromJdbcConfig(hiveJdbcConfig, standardizationConfig.hiveOptimizeExistQuery)
          case None                 =>
            log.info(s"Using Hive SQL API by connecting to the Hive metastore via Spark.")
            QueryExecutorSpark(spark)
        }
        new HiveHelperSql(queryExecutor, hiveConfig, alwaysEscapeColumnNames)
      case HiveApi.SparkCatalog =>
        log.info(s"Using Hive via Spark Catalog API and configuration.")
        new HiveHelperSparkCatalog(spark)
    }

    new StandardizationSink(conf, standardizationConfig, hiveHelper)
  }
}
