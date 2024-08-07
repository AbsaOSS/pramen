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
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.Query
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.utils.Emoji.SUCCESS
import za.co.absa.pramen.core.utils.hive.QueryExecutor
import za.co.absa.pramen.core.utils.{FsUtils, StringUtils}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try

class MetastorePersistenceDelta(query: Query,
                                infoDateColumn: String,
                                infoDateFormat: String,
                                recordsPerPartition: Option[Long],
                                saveModeOpt: Option[SaveMode],
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
      case _                  => throw new IllegalArgumentException(s"Arguments of type '${query.name}' are not supported for Delta format. Use either 'path' or 'table'.")
    }

    df.filter(getFilter(infoDateFrom, infoDateTo))
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    val infoDateStr = dateFormatter.format(infoDate)

    val dfIn = df.withColumn(infoDateColumn, lit(infoDateStr).cast(DateType))

    val whereCondition = s"$infoDateColumn='$infoDateStr'"

    val recordCount = numberOfRecordsEstimate match {
      case Some(count) => count
      case None => dfIn.count()
    }

    val dfRepartitioned = applyRepartitioning(dfIn, recordCount)

    val saveMode = saveModeOpt.getOrElse(SaveMode.Overwrite)

    val operationStr = saveMode match {
      case SaveMode.Append => "Appending to"
      case _               => "Writing to"
    }

    if (log.isDebugEnabled) {
      log.debug(s"Schema: ${dfIn.schema.treeString}")
      log.debug(s"Info date column: $infoDateColumn")
      log.debug(s"Info date: $infoDateStr")
    }

    val writer = dfRepartitioned
      .write
      .format("delta")
      .mode(saveMode)
      .partitionBy(infoDateColumn)
      .option("mergeSchema", "true")
      .option("replaceWhere", s"$infoDateColumn='$infoDateStr'")
      .options(writeOptions)

    query match {
      case Query.Path(path)   =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)
        fsUtils.createDirectoryRecursive(new Path(path))
        log.info(s"$operationStr to path '$path' WHERE $whereCondition...")
        writer.save(path)
      case Query.Table(table) =>
        writer.saveAsTable(table)
        log.info(s"$operationStr to table '$table' WHERE $whereCondition...")
      case q =>
        throw new IllegalStateException(s"The '${q.name}' option is not supported as a write target for Delta.")
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
        val size = Try {
          // When 0 records is saved to a Delta directory, the directory is not created.
          // But it is not an exceptional situation.
          fsUtils.getDirectorySize(getPartitionPath(infoDate).toUri.toString)
        }.getOrElse(0L)
        Some(size)
      case Query.Table(_) =>
        None
      case _ =>
        None
    }

    MetaTableStats(recordCount, sizeOpt)
  }

  override def createOrUpdateHiveTable(infoDate: LocalDate,
                                       hiveTableName: String,
                                       queryExecutor: QueryExecutor,
                                       hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Delta format does not support Hive tables at the moment.")
  }

  override def repairHiveTable(hiveTableName: String,
                                queryExecutor: QueryExecutor,
                                hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Delta format does not support Hive tables at the moment.")
  }

  def getFilter(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): Column = {
    (infoDateFrom, infoDateTo) match {
      case (None, None)           => log.warn(s"Reading '${query.query}' without filters. This can have performance impact."); lit(true)
      case (Some(from), None)     => col(infoDateColumn) >= lit(dateFormatter.format(from))
      case (None, Some(to))       => col(infoDateColumn) <= lit(dateFormatter.format(to))
      case (Some(from), Some(to)) => col(infoDateColumn) >= lit(dateFormatter.format(from)) && col(infoDateColumn) <= lit(dateFormatter.format(to))
    }
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

