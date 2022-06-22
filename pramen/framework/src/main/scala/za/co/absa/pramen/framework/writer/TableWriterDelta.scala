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

package za.co.absa.pramen.framework.writer

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.v2.Query
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.framework.model.Constants
import za.co.absa.pramen.framework.utils.{ConfigUtils, FsUtils, StringUtils}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable

class TableWriterDelta(infoDateColumn: String,
                       infoDateFormat: String,
                       outputQuery: Query,
                       recordsPerPartition: Long,
                       extraOptions: Map[String, String])
                      (implicit spark: SparkSession) extends TableWriter {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)
  private val metadata = new mutable.HashMap[String, Any]()

  override def write(df: DataFrame, infoDate: LocalDate, numOfRecordsEstimate: Option[Long]): Long = {
    val infoDateStr = dateFormatter.format(infoDate)
    val dfIn = addInfoDateColumn(df, infoDateStr)

    val dfToWrite = repartition(dfIn, numOfRecordsEstimate)

    val writer = dfToWrite
      .write
      .format("delta")
      .partitionBy(infoDateColumn)
      .mode(SaveMode.Overwrite)
      .option("mergeSchema", "true")
      .option("replaceWhere", s"$infoDateColumn='$infoDateStr'")
      .options(extraOptions)

    outputQuery match {
      case Query.Path(outputDirectory) =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, outputDirectory)

        fsUtils.createDirectoryRecursive(new Path(outputDirectory))
        log.info(s"Writing to '$outputDirectory'...")
        writer.save(outputDirectory)
      case Query.Table(table)          =>
        writer.saveAsTable(table)
      case _                           => throw new IllegalArgumentException(s"Arguments of type ${outputQuery.getClass} are not supported for Delta format.")
    }

    val actualCount = getActualCount(infoDateStr)

    val size = getActualSize(infoDateStr)

    val prettySize = size.map(s => {
      metadata.put(Constants.METADATA_LAST_SIZE_WRITTEN, s)
      s"(${StringUtils.prettySize(s)})"
    })

    log.info(s"Successfully saved $actualCount records $prettySize to ${outputQuery.query}")
    actualCount
  }

  override def getMetadata(key: String): Option[Any] = {
    metadata.get(key)
  }

  def getExtraOptions: Map[String, String] = extraOptions

  private def addInfoDateColumn(df: DataFrame, infoDateStr: String): DataFrame = {
    if (df.schema.exists(_.name.equalsIgnoreCase(infoDateColumn))) {
      df.drop(infoDateColumn)
        .withColumn(infoDateColumn, lit(infoDateStr))
    } else {
      df.withColumn(infoDateColumn, lit(infoDateStr))
    }
  }

  private def repartition(dfIn: DataFrame, numOfRecordsEstimate: Option[Long]): DataFrame = {
    val numPartitions = numOfRecordsEstimate.map(numOfRecords => Math.max(1, Math.ceil(numOfRecords.toDouble / recordsPerPartition)).toInt)

    numPartitions match {
      case Some(n) => dfIn.repartition(n)
      case None    => dfIn
    }
  }

  private def getActualCount(infoDateStr: String) = {
    val df = outputQuery match {
      case Query.Path(path)   =>
        spark.read
          .format("delta")
          .load(path)
      case Query.Table(table) =>
        spark
          .table(table)
      case _                  => throw new IllegalArgumentException(s"Arguments of type ${outputQuery.getClass} are not supported for Delta format.")
    }

    df.filter(col(infoDateColumn) === infoDateStr)
      .count
  }

  private def getActualSize(infoDateStr: String): Option[Long] = {
    outputQuery match {
      case Query.Path(path) =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)
        Option(fsUtils.getDirectorySize(new Path(path, s"$infoDateColumn=$infoDateStr").toString))
      case _ => None
    }
  }
}

object TableWriterDelta {
  def apply(infoDateColumn: String,
            infoDateFormat: String,
            outputQuery: Query,
            recordsPerPartition: Long,
            extraConf: Config,
            parent: String)(implicit spark: SparkSession): TableWriterDelta = {
    val extraOptions = ConfigUtils.getExtraOptions(extraConf, "option")

    new TableWriterDelta(infoDateColumn,
      infoDateFormat,
      outputQuery,
      recordsPerPartition,
      extraOptions
    )
  }
}
