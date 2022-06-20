/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.metastore.peristence

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.metastore.MetaTableStats
import za.co.absa.pramen.framework.utils.{FsUtils, StringUtils}

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class MetastorePersistenceParquet(path: String,
                                  infoDateColumn: String,
                                  infoDateFormat: String,
                                  recordsPerPartition: Option[Long],
                                  readOptions: Map[String, String],
                                  writeOptions: Map[String, String]
                                 )(implicit spark: SparkSession) extends MetastorePersistence {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    if (infoDateFrom.isDefined && infoDateTo.isDefined && infoDateFrom.get == infoDateTo.get && hasData(infoDateFrom.get)) {
      loadPartitionDirectly(infoDateFrom.get)
    } else {
      loadTableFromRootFolder(infoDateFrom, infoDateTo)
    }
  }

  def loadPartitionDirectly(infoDate: LocalDate): DataFrame = {
    val dateStr = dateFormatter.format(infoDate)
    val partitionPath = getPartitionPath(infoDate)

    log.info(s"Partition column exists, reading from $partitionPath.")

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
    spark.read
      .format("parquet")
      .options(readOptions)
      .load(path)
      .filter(getFilter(infoDateFrom, infoDateTo))
  }

  def getFilter(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): Column = {
    (infoDateFrom, infoDateTo) match {
      case (None, None)           => log.warn(s"Reading '$path' without filters. This can have performance impact."); lit(true)
      case (Some(from), None)     => col(infoDateColumn) >= lit(dateFormatter.format(from))
      case (None, Some(to))       => col(infoDateColumn) <= lit(dateFormatter.format(to))
      case (Some(from), Some(to)) => col(infoDateColumn) >= lit(dateFormatter.format(from)) && col(infoDateColumn) <= lit(dateFormatter.format(to))
    }
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    val outputDir = getPartitionPath(infoDate)
    val outputDirStr = getPartitionPath(infoDate).toUri.toString

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)

    fsUtils.createDirectoryRecursive(new Path(path))

    val dfIn = if (df.schema.exists(_.name.equalsIgnoreCase(infoDateColumn))) {
      df.drop(infoDateColumn)
    } else {
      df
    }

    log.info(s"Writing to '$outputDirStr'...")

    val recordCount = numberOfRecordsEstimate match {
      case Some(count) => count
      case None => dfIn.count()
    }

    val dfRepartitioned = applyRepartitioning(dfIn, recordCount)

    dfRepartitioned
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .options(writeOptions)
      .save(outputDirStr)

    val stats = getStats(infoDate)

    log.info(s"Successfully saved ${stats.recordCount} records (${StringUtils.prettySize(stats.dataSizeBytes.get)}) to $outputDir")

    stats
  }

  override def getStats(infoDate: LocalDate): MetaTableStats = {
    val outputDirStr = getPartitionPath(infoDate).toUri.toString

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)

    val actualCount = spark.read.parquet(outputDirStr).count()

    val size = fsUtils.getDirectorySize(outputDirStr)

    MetaTableStats(actualCount, Option(size))
  }

  def applyRepartitioning(dfIn: DataFrame, recordCount: Long): DataFrame = {
    recordsPerPartition match {
      case None      => dfIn
      case Some(rpp) =>
        val numPartitions = Math.max(1, Math.ceil(recordCount.toDouble / rpp)).toInt
        dfIn.repartition(numPartitions)
    }
  }

  private def hasData(infoDate: LocalDate): Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val path = getPartitionPath(infoDate)
    val hasPartition = fs.exists(path)
    if (!hasPartition) {
      log.warn(s"No partition: $path")
    }
    hasPartition
  }

  private def getPartitionPath(infoDate: LocalDate): Path = {
    val partition = s"$infoDateColumn=${dateFormatter.format(infoDate)}"
    new Path(path, partition)
  }
}
