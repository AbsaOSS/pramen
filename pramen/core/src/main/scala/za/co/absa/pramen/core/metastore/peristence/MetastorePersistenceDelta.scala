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
import za.co.absa.pramen.api.Query
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.utils.Emoji.SUCCESS
import za.co.absa.pramen.core.utils.{FsUtils, StringUtils}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class MetastorePersistenceDelta(query: Query,
                                infoDateColumn: String,
                                infoDateFormat: String,
                                recordsPerPartition: Option[Long],
                                readOptions: Map[String, String],
                                writeOptions: Map[String, String]
                                 )(implicit spark: SparkSession) extends MetastorePersistence {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    val df = query match {
      case Query.Path(path)   =>
        spark.read
          .format("delta")
          .options(readOptions)
          .load(path)
      case Query.Table(table) =>
        spark.table(table)
      case _                  => throw new IllegalArgumentException(s"Arguments of type ${query.getClass} are not supported for Delta format.")
    }

    df.filter(getFilter(infoDateFrom, infoDateTo))
  }

  def getFilter(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): Column = {
    (infoDateFrom, infoDateTo) match {
      case (None, None)           => log.warn(s"Reading '${query.query}' without filters. This can have performance impact."); lit(true)
      case (Some(from), None)     => col(infoDateColumn) >= lit(dateFormatter.format(from))
      case (None, Some(to))       => col(infoDateColumn) <= lit(dateFormatter.format(to))
      case (Some(from), Some(to)) => col(infoDateColumn) >= lit(dateFormatter.format(from)) && col(infoDateColumn) <= lit(dateFormatter.format(to))
    }
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    val infoDateStr = dateFormatter.format(infoDate)

    val dfIn = df.withColumn(infoDateColumn, lit(infoDateStr))

    val whereCondition = s"$infoDateColumn='$infoDateStr'"

    val recordCount = numberOfRecordsEstimate match {
      case Some(count) => count
      case None => dfIn.count()
    }

    val dfRepartitioned = applyRepartitioning(dfIn, recordCount)

    log.debug(s"Schema: ${dfIn.schema.treeString}")
    log.debug(s"Info date column: $infoDateColumn")
    log.debug(s"Info date: $infoDateStr")

    val writer = dfRepartitioned
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .partitionBy(infoDateColumn)
      .option("mergeSchema", "true")
      .option("replaceWhere", s"$infoDateColumn='$infoDateStr'")
      .options(writeOptions)

    query match {
      case Query.Path(path)   =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)
        fsUtils.createDirectoryRecursive(new Path(path))
        log.info(s"Writing to path '$path' WHERE $whereCondition...")
        writer.save(path)
      case Query.Table(table) =>
        writer.saveAsTable(table)
        log.info(s"Writing to table '$table' WHERE $whereCondition...")
      case Query.Sql(_) =>
        throw new IllegalStateException(s"An sql expression is not supported as a write target for Delta.")
    }

    val stats = getStats(infoDate)

    stats.dataSizeBytes match {
      case Some(size) => log.info(s"$SUCCESS Successfully saved ${stats.recordCount} records (${StringUtils.prettySize(size)}) to ${query.query}")
      case None => log.info(s"$SUCCESS Successfully saved ${stats.recordCount} records to ${query.query}")
    }

    stats
  }

  override def getStats(infoDate: LocalDate): MetaTableStats = {
    val df = loadTable(Option(infoDate), Option(infoDate))
    val recordCount = df.count()

    val sizeOpt = query match {
      case Query.Path(path)   =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)
        val size = fsUtils.getDirectorySize(getPartitionPath(infoDate).toUri.toString)
        Some(size)
      case Query.Table(_) =>
        None
      case _ =>
        None
    }

    MetaTableStats(recordCount, sizeOpt)
  }

  def applyRepartitioning(dfIn: DataFrame, recordCount: Long): DataFrame = {
    recordsPerPartition match {
      case None      => dfIn
      case Some(rpp) =>
        val numPartitions = Math.max(1, Math.ceil(recordCount.toDouble / rpp)).toInt
        dfIn.repartition(numPartitions)
    }
  }

  private def getPartitionPath(infoDate: LocalDate): Path = {
    val partition = s"$infoDateColumn=${dateFormatter.format(infoDate)}"
    new Path(query.query, partition)
  }
}

