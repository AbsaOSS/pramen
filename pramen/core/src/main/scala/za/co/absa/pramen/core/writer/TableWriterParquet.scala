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

package za.co.absa.pramen.core.writer

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.TableWriter
import za.co.absa.pramen.core.utils.{FsUtils, PartitionUtils}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable

class TableWriterParquet(infoDateColumn: String,
                         infoDateFormat: String,
                         baseOutputDirectory: String,
                         temporaryDirectory: String,
                         recordsPerPartition: Long,
                         customPartitionPattern: Option[String])
                        (implicit spark: SparkSession) extends TableWriter {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)

  override def write(df: DataFrame, infoDate: LocalDate, numOfRecordsEstimate: Option[Long]): Long = {
    val outputDir = getPartitionPath(infoDate)
    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, outputDir)

    fsUtils.createDirectoryRecursiveButLast(new Path(outputDir))

    val numPartitions = numOfRecordsEstimate.map(numOfRecords => Math.max(1, Math.ceil(numOfRecords.toDouble / recordsPerPartition)).toInt)

    val dfIn = if (df.schema.exists(_.name.equalsIgnoreCase(infoDateColumn))) {
      df.drop(infoDateColumn)
    } else {
      df
    }

    log.info(s"Writing to '$outputDir'...")

    val dfRepartitioned = numPartitions match {
      case Some(n) => dfIn.repartition(n)
      case None    => dfIn
    }

    dfRepartitioned
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputDir)

    val rawDf = spark.read.parquet(outputDir)
    rawDf.count()
  }

  private def getPartitionPath(date: LocalDate): String = {
    val partition = customPartitionPattern match {
      case Some(pattern) => PartitionUtils.unpackCustomPartitionPattern(pattern, infoDateColumn, date, 1)
      case None          => s"$infoDateColumn=${dateFormatter.format(date)}"
    }
    new Path(baseOutputDirectory, partition).toUri.toString
  }
}

object TableWriterParquet {
  def apply(infoDateColumn: String,
            infoDateFormat: String,
            baseOutputDirectory: String,
            temporaryDirectory: String,
            recordsPerPartition: Long,
            extraConf: Config,
            parent: String,
            customPartitionPattern: Option[String])(implicit spark: SparkSession): TableWriterParquet = {

    new TableWriterParquet(infoDateColumn,
      infoDateFormat,
      baseOutputDirectory,
      temporaryDirectory,
      recordsPerPartition,
      customPartitionPattern)
  }
}
