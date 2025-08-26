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

package za.co.absa.pramen.core.metastore.peristence

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{PartitionInfo, PartitionScheme}
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.utils.Emoji.SUCCESS
import za.co.absa.pramen.core.utils.hive.QueryExecutor
import za.co.absa.pramen.core.utils.{ConfigUtils, FsUtils, SparkUtils, StringUtils}

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try
import scala.util.control.NonFatal

class MetastorePersistenceParquet(path: String,
                                  infoDateColumn: String,
                                  infoDateFormat: String,
                                  batchIdColumn: String,
                                  batchId: Long,
                                  partitionInfo: PartitionInfo,
                                  partitionScheme: PartitionScheme,
                                  saveModeOpt: Option[SaveMode],
                                  readOptions: Map[String, String],
                                  writeOptions: Map[String, String]
                                 )(implicit spark: SparkSession) extends MetastorePersistence {

  import MetastorePersistenceParquet._

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    if (infoDateFrom.isDefined &&
      infoDateTo.isDefined &&
      infoDateFrom.get == infoDateTo.get &&
      SparkUtils.hasDataInPartition(infoDateFrom.get, infoDateColumn, infoDateFormat, path)) {
      loadPartitionDirectly(infoDateFrom.get)
    } else {
      loadTableFromRootFolder(infoDateFrom, infoDateTo)
    }
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    val outputDir = if (partitionScheme == PartitionScheme.Overwrite)
      new Path(path)
    else
      SparkUtils.getPartitionPath(infoDate, infoDateColumn, infoDateFormat, path)
    val outputDirStr = outputDir.toUri.toString

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)

    fsUtils.createDirectoryRecursive(new Path(path))

    val saveMode = saveModeOpt.getOrElse(SaveMode.Overwrite)

    val isAppend = saveMode match {
      case SaveMode.Append =>
        log.info(s"Appending to '$outputDirStr'...")
        true
      case _               =>
        log.info(s"Writing to '$outputDirStr'...")
        false
    }

    val dfIn = if (partitionScheme != PartitionScheme.Overwrite) {
      df.drop(infoDateColumn)
    } else {
      df.withColumn(infoDateColumn, lit(Date.valueOf(infoDate)))
    }

    val dfRepartitioned = applyPartitioning(dfIn, partitionInfo, numberOfRecordsEstimate)

    writeAndCleanOnFailure(dfRepartitioned, outputDirStr, fsUtils, saveMode)

    val stats = getStats(infoDate, isAppend)

    stats.recordCountAppended match {
      case Some(recordsAppended) =>
        log.info(s"$SUCCESS Successfully saved $recordsAppended records (new count: ${stats.recordCount.get}, " +
          s"new size: ${StringUtils.prettySize(stats.dataSizeBytes.get)}) to $outputDir")
      case None =>
        log.info(s"$SUCCESS Successfully saved ${stats.recordCount.get} records " +
          s"(${StringUtils.prettySize(stats.dataSizeBytes.get)}) to $outputDir")
    }

    stats
  }

  override def getStats(infoDate: LocalDate, onlyForCurrentBatchId: Boolean): MetaTableStats = {
    val outputDirStr = if (partitionScheme == PartitionScheme.Overwrite)
      path
    else
      SparkUtils.getPartitionPath(infoDate, infoDateColumn, infoDateFormat, path).toUri.toString

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)

    val df = spark.read.parquet(outputDirStr)

    val size = fsUtils.getDirectorySize(outputDirStr)

    if (onlyForCurrentBatchId && df.schema.exists(_.name.equalsIgnoreCase(batchIdColumn))) {
      val batchCount = df.filter(col(batchIdColumn) === batchId).count()
      val countAll = df.count()

      MetaTableStats(Option(countAll), Option(batchCount), Option(size))
    } else {
      val countAll = df.count()

      MetaTableStats(Option(countAll), None, Option(size))
    }
  }

  override def createOrUpdateHiveTable(infoDate: LocalDate,
                                       hiveTableName: String,
                                       queryExecutor: QueryExecutor,
                                       hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Parquet format does not support Hive tables at the moment.")
  }

  override def repairHiveTable(hiveTableName: String,
                               queryExecutor: QueryExecutor,
                               hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Parquet format does not support Hive tables at the moment.")
  }

  def loadPartitionDirectly(infoDate: LocalDate): DataFrame = {
    val dateStr = dateFormatter.format(infoDate)
    val partitionPath = SparkUtils.getPartitionPath(infoDate, infoDateColumn, infoDateFormat, path)

    log.info(s"Partition column exists, reading from $partitionPath.")

    if (readOptions.nonEmpty) {
      log.info("Custom read options:")
      ConfigUtils.renderExtraOptions(readOptions, Keys.KEYS_TO_REDACT)(log.info)
    }

    val dfIn = spark.read
      .format("parquet")
      .options(readOptions)
      .load(partitionPath.toUri.toString)

    if (dfIn.schema.fields.exists(_.name == infoDateColumn)) {
      log.warn(s"Partition column $infoDateColumn is duplicated in data itself for $dateStr.")
      dfIn
    } else {
      // If the partition column does not exist in the schema (which is expected), add that column
      dfIn.withColumn(infoDateColumn, lit(Date.valueOf(infoDate)))
    }
  }

  def loadTableFromRootFolder(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    if (readOptions.nonEmpty) {
      log.info("Custom read options:")
      ConfigUtils.renderExtraOptions(readOptions, Keys.KEYS_TO_REDACT)(log.info)
    }

    spark.read
      .format("parquet")
      .options(readOptions)
      .load(path)
      .filter(getFilter(infoDateFrom, infoDateTo))
  }

  def getFilter(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): Column = {
    if (partitionScheme == PartitionScheme.Overwrite) {
      if (infoDateFrom.isDefined || infoDateTo.isDefined) {
        log.warn(s"Date filter is ignored when the partition scheme is 'Overwrite' for '$path'")
      }
      lit(true)
    } else {
      (infoDateFrom, infoDateTo) match {
        case (None, None) => log.warn(s"Reading '$path' without filters. This can have performance impact."); lit(true)
        case (Some(from), None) => col(infoDateColumn) >= lit(dateFormatter.format(from))
        case (None, Some(to)) => col(infoDateColumn) <= lit(dateFormatter.format(to))
        case (Some(from), Some(to)) => col(infoDateColumn) >= lit(dateFormatter.format(from)) && col(infoDateColumn) <= lit(dateFormatter.format(to))
      }
    }
  }

  private[core] def writeAndCleanOnFailure(df: DataFrame,
                                           outputDirStr: String,
                                           fsUtils: FsUtils,
                                           saveMode: SaveMode): Unit = {
    if (writeOptions.nonEmpty) {
      log.info("Custom write options:")
      ConfigUtils.renderExtraOptions(writeOptions, Keys.KEYS_TO_REDACT)(log.info)
    }

    try {
      df.write
        .mode(saveMode)
        .format("parquet")
        .options(writeOptions)
        .save(outputDirStr)
    } catch {
      case NonFatal(ex) =>
        // Failure to save the dataframe here creates an empty directory, which is not a valid partition.
        // We need to delete it to avoid an attempt to read it in the future.
        Try {
          val partitionPath = new Path(outputDirStr)
          if (fsUtils.exists(partitionPath) && saveMode == SaveMode.Overwrite && partitionScheme != PartitionScheme.Overwrite) {
            log.warn(s"The write failed. Deleting the empty directory: $partitionPath")
            fsUtils.deleteDirectoryRecursively(partitionPath)
          }
        }

        throw ex
    }
  }
}

object MetastorePersistenceParquet {
  def applyPartitioning(dfIn: DataFrame, partitionInfo: PartitionInfo, recordCountEstimate: Option[Long]): DataFrame = {
    partitionInfo match {
      case PartitionInfo.Default => dfIn
      case PartitionInfo.Explicit(nop) =>
        dfIn.coalesce(nop)
      case PartitionInfo.PerRecordCount(rpp) =>
        val recordCount = recordCountEstimate.getOrElse(dfIn.count())
        val numPartitions = Math.max(1, Math.ceil(recordCount.toDouble / rpp)).toInt
        dfIn.repartition(numPartitions)
    }
  }
}
