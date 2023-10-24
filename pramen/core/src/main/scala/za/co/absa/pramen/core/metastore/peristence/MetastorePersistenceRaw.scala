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

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.utils.hive.QueryExecutor
import za.co.absa.pramen.core.utils.{FsUtils, SparkUtils}

import java.time.LocalDate
import scala.collection.mutable

class MetastorePersistenceRaw(path: String,
                              infoDateColumn: String,
                              infoDateFormat: String
                             )(implicit spark: SparkSession) extends MetastorePersistence {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    import spark.implicits._

    (infoDateFrom, infoDateTo) match {
      case (Some(from), Some(to)) if from.isEqual(to) =>
        getListOfFiles(from).toDF("path")
      case (Some(from), Some(to)) =>
        getListOfFilesRange(from, to).toDF("path")
      case _ =>
        throw new IllegalArgumentException("Metastore 'raw' format requires info date for querying its contents.")
    }
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    if (!df.schema.exists(_.name == "path")) {
      throw new IllegalArgumentException("The 'raw' persistent format data frame should have 'path' column.")
    }

    val files = df.select("path").collect().map(_.getString(0))

    val outputDir = SparkUtils.getPartitionPath(infoDate, infoDateColumn, infoDateFormat, path)

    val fsUtilsTrg = new FsUtils(spark.sparkContext.hadoopConfiguration, outputDir.toString)

    if (fsUtilsTrg.exists(outputDir)) {
      fsUtilsTrg.deleteDirectoryRecursively(outputDir)
    }

    fsUtilsTrg.createDirectoryRecursive(outputDir)

    var totalSize = 0L

    if (files.isEmpty) {
      log.info("Nohting to save")
    } else {
      files.foreach(file => {
        val srcPath = new Path(file)
        val trgPath = new Path(outputDir, srcPath.getName)
        val fsSrc = srcPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

        log.info(s"Copying file from $srcPath to $trgPath")

        totalSize += fsSrc.getContentSummary(srcPath).getLength
        fsUtilsTrg.copyFile(srcPath, trgPath)
      })
    }

    MetaTableStats(
      files.length,
      Some(totalSize)
    )
  }

  override def getStats(infoDate: LocalDate): MetaTableStats = {
    val partitionDir = SparkUtils.getPartitionPath(infoDate, infoDateColumn, infoDateFormat, path)

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)

    val files = fsUtils.getHadoopFiles(partitionDir)

    var totalSize = 0L

    files.foreach(file => {
      totalSize += fsUtils.fs.getContentSummary(new Path(file)).getLength
    })

    MetaTableStats(
      files.length,
      Some(totalSize)
    )
  }

  override def createOrUpdateHiveTable(infoDate: LocalDate,
                                       hiveTableName: String,
                                       queryExecutor: QueryExecutor,
                                       hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Raw format does not support Hive tables.")
  }

  override def repairHiveTable(hiveTableName: String,
                               queryExecutor: QueryExecutor,
                               hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Raw format does not support Hive tables.")
  }

  private def getListOfFilesRange(infoDateFrom: LocalDate, infoDateTo: LocalDate): Seq[String] = {
    if (infoDateFrom.isAfter(infoDateTo))
      Seq.empty[String]
    else {
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)
      var d = infoDateFrom
      val files = mutable.ArrayBuffer.empty[String]

      while (d.isBefore(infoDateTo) || d.isEqual(infoDateTo)) {
        val subPath = SparkUtils.getPartitionPath(d, infoDateColumn, infoDateFormat, path)
        if (fsUtils.exists(subPath)) {
          files ++= fsUtils.getHadoopFiles(subPath)
        }
        d = d.plusDays(1)
      }
      files.toSeq
    }
  }

  private def getListOfFiles(infoDate: LocalDate): Seq[String] = {
    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)

    val subPath = SparkUtils.getPartitionPath(infoDate, infoDateColumn, infoDateFormat, path)

    if (fsUtils.exists(new Path(path)) && !fsUtils.exists(subPath)) {
      // The absence of the partition folder means no data is there, which is okay quite often.
      // But fsUtils.getHadoopFiles() throws an exception that fails the job and dependent jobs in this case
      Seq.empty[String]
    } else {
      fsUtils.getHadoopFiles(subPath).toSeq
    }
  }
}
