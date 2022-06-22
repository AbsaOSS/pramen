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
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.plugins.{IngestionContext, PostProcessingPlugin}
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.framework.model.Constants
import za.co.absa.pramen.framework.utils.{FsUtils, PartitionUtils, StringUtils}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}
import scala.collection.mutable

class TableWriterParquet(infoDateColumn: String,
                         infoDateFormat: String,
                         baseOutputDirectory: String,
                         temporaryDirectory: String,
                         recordsPerPartition: Long,
                         customPartitionPattern: Option[String],
                         postProcessPlugin: Seq[PostProcessingPlugin])
                        (implicit spark: SparkSession) extends TableWriter {
  // ToDo Add support for extra Spark options in 0.9.0

  // This constructor is used for backwards compatibility
  def this(infoDateColumn: String,
           infoDateFormat: String,
           baseOutputDirectory: String,
           temporaryDirectory: String,
           recordsPerPartition: Long
          )(implicit spark: SparkSession) {
    this(infoDateColumn, infoDateFormat, baseOutputDirectory, temporaryDirectory, recordsPerPartition, None, Seq.empty[PostProcessingPlugin])
  }

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)
  private val metadata = new mutable.HashMap[String, Any]()

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

    val sourceStart = Instant.now()
    dfRepartitioned
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputDir)

    postProcess(infoDate, outputDir, numOfRecordsEstimate, sourceStart)
  }

  override def getMetadata(key: String): Option[Any] = {
    metadata.get(key)
  }

  private def getPartitionPath(date: LocalDate): String = {
    val partition = customPartitionPattern match {
      case Some(pattern) => PartitionUtils.unpackCustomPartitionPattern(pattern, infoDateColumn, date, 1)
      case None          => s"$infoDateColumn=${dateFormatter.format(date)}"
    }
    new Path(baseOutputDirectory, partition).toUri.toString
  }

  private def postProcess(infoDate: LocalDate,
                          outputDir: String,
                          numOfRecordsEstimate: Option[Long],
                          sourceStart: Instant): Long = {
    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, outputDir)

    val rawStart = Instant.now()
    val rawDf = spark.read.parquet(outputDir)
    val actualCount = rawDf.count()
    val context = IngestionContext(numOfRecordsEstimate.getOrElse(actualCount), actualCount, new Path(outputDir), infoDate, sourceStart, rawStart)
    postProcessPlugin.foreach(p => p.postProcess(context, rawDf))

    val size = fsUtils.getDirectorySize(outputDir)
    metadata.put(Constants.METADATA_LAST_SIZE_WRITTEN, size)

    log.info(s"Successfully saved $actualCount records (${StringUtils.prettySize(size)}) to $outputDir")

    actualCount
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
            customPartitionPattern: Option[String],
            postProcessPlugin: Seq[PostProcessingPlugin])(implicit spark: SparkSession): TableWriterParquet = {

    new TableWriterParquet(infoDateColumn,
      infoDateFormat,
      baseOutputDirectory,
      temporaryDirectory,
      recordsPerPartition,
      customPartitionPattern,
      postProcessPlugin)
  }
}
