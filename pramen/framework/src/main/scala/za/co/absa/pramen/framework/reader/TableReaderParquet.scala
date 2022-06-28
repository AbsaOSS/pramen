/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.pramen.framework.reader

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.TableReader

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class TableReaderParquet(path: String,
                         hasInfoDateColumn: Boolean,
                         infoDateColumn: String,
                         infoDateFormat: String = "yyyy-MM-dd",
                         options: Map[String, String] = Map.empty[String, String]
                        )(implicit spark: SparkSession) extends TableReader {

  // This constructor is used for backwards compatibility
  def this(path: String,
           infoDateColumn: String,
           infoDateFormat: String
          )(implicit spark: SparkSession) {
    this(path, true, infoDateColumn, infoDateFormat);
  }

  // This constructor is used for backwards compatibility
  def this(path: String,
           infoDateColumn: String
          )(implicit spark: SparkSession) {
    this(path, true, infoDateColumn, "yyyy-MM-dd");
  }

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)

  override def getRecordCount(infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    if (hasInfoDateColumn) {
      if (infoDateBegin.equals(infoDateEnd)) {
        if (hasData(infoDateEnd)) {
          log.info(s"Reading COUNT(*) FROM $path WHERE $infoDateColumn='${dateFormatter.format(infoDateBegin)}'")
          getDailyDataFrame(infoDateBegin).count
        } else {
          log.info(s"No partition for $path with $infoDateColumn='${dateFormatter.format(infoDateBegin)}' has been created yet.")
          0L
        }
      } else {
        log.info(s"Reading COUNT(*) FROM $path WHERE $infoDateColumn BETWEEN '${dateFormatter.format(infoDateBegin)}' AND '${dateFormatter.format(infoDateEnd)}'")
        getFilteredDataFrame(infoDateBegin, infoDateEnd).count
      }
    } else {
      getUnfilteredDataFrame.count()
    }
  }

  override def getData(infoDateBegin: LocalDate, infoDateEnd: LocalDate): Option[DataFrame] = {
    if (hasInfoDateColumn) {
      if (infoDateBegin.equals(infoDateEnd)) {
        log.info(s"Reading * FROM $path WHERE $infoDateColumn='${dateFormatter.format(infoDateEnd)}'")
        if (hasData(infoDateEnd)) {
          Option(getDailyDataFrame(infoDateEnd))
        } else {
          None
        }
      } else {
        log.info(s"Reading * FROM $path WHERE $infoDateColumn BETWEEN '${dateFormatter.format(infoDateBegin)}' AND '${dateFormatter.format(infoDateEnd)}'")
        Option(getFilteredDataFrame(infoDateBegin, infoDateEnd))
      }
    } else {
      Option(getUnfilteredDataFrame)
    }
  }

  private def getDailyDataFrame(infoDate: LocalDate): DataFrame = {
    val dateStr = dateFormatter.format(infoDate)

    if (hasData(infoDate)) {
      // If the partition folder exists, read directly from it
      val partitionPath = getPartitionPath(infoDate)
      log.info(s"Partition column exists, reading from $partitionPath.")

      val dfIn = spark
        .read
        .parquet(partitionPath.toUri.toString)

      if (dfIn.schema.fields.exists(_.name == infoDateColumn)) {
        log.warn(s"Partition column $infoDateColumn is duplicated in data itself for $dateStr.")
        dfIn
      } else {
        // If the partition column does not exist in the schema (which is expected), add that column
        dfIn.withColumn(infoDateColumn, lit(dateStr))
      }
    } else {
      log.info(s"Reading data from $path filtered by $infoDateColumn = '$dateStr'.")
      getFilteredDataFrame(infoDate, infoDate)
    }
  }

  private def getFilteredDataFrame(infoDateBegin: LocalDate, infoDateEnd: LocalDate): DataFrame = {
    val infoDateBeginStr = dateFormatter.format(infoDateBegin)
    val infoDateEndStr = dateFormatter.format(infoDateEnd)
    if (infoDateBegin.equals(infoDateEnd)) {
      spark
        .read
        .options(options)
        .parquet(path)
        .filter(col(s"$infoDateColumn") === lit(infoDateBeginStr))
    } else {
      spark
        .read
        .options(options)
        .parquet(path)
        .filter(col(s"$infoDateColumn") >= lit(infoDateBeginStr) && col(s"$infoDateColumn") <= lit(infoDateEndStr))
    }
  }

  private def getUnfilteredDataFrame: DataFrame = {
    spark
      .read
      .options(options)
      .parquet(path)
  }

  private def hasData(infoDate: LocalDate): Boolean = {
    val path = getPartitionPath(infoDate)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)

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
