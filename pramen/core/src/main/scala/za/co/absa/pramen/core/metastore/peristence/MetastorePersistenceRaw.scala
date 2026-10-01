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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SerializableWritable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.PartitionScheme
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.utils.hive.QueryExecutor
import za.co.absa.pramen.core.utils.{FsUtils, SparkUtils}

import java.time.LocalDate
import scala.collection.mutable

class MetastorePersistenceRaw(path: String,
                              infoDateColumn: String,
                              infoDateFormat: String,
                              partitionScheme: PartitionScheme,
                              saveModeOpt: Option[SaveMode],
                              copyOnDriverOpt: Option[Boolean])
                             (implicit spark: SparkSession) extends MetastorePersistence {

  import MetastorePersistenceRaw._
  import spark.implicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    (infoDateFrom, infoDateTo) match {
      case _ if partitionScheme == PartitionScheme.Overwrite =>
        listOfPathsToDf(getListOfFiles)
      case (Some(from), Some(to)) if from.isEqual(to) =>
        listOfPathsToDf(getListOfFiles(from))
      case (Some(from), Some(to)) =>
        listOfPathsToDf(getListOfFilesRange(from, to))
      case _ =>
        throw new IllegalArgumentException("Metastore 'raw' format requires info date for querying its contents.")
    }
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    if (!df.schema.exists(_.name == RAW_PATH_FIELD_KEY)) {
      throw new IllegalArgumentException("The 'raw' persistent format data frame should have 'path' column.")
    }

    val files = RawFile.fromDf(df)

    val outputDir = if (partitionScheme == PartitionScheme.Overwrite)
      new Path(path)
    else
      SparkUtils.getPartitionPath(infoDate, infoDateColumn, infoDateFormat, path)


    val fsUtilsTrg = new FsUtils(spark.sparkContext.hadoopConfiguration, outputDir.toString)

    if (fsUtilsTrg.exists(outputDir)) {
      if (saveModeOpt.contains(SaveMode.Append)) {
        log.info(s"Appending to partition: $outputDir...")
      } else {
        log.info(s"Cleaning up the partition: $outputDir...")
        fsUtilsTrg.deleteDirectoryRecursively(outputDir)
      }
    }

    fsUtilsTrg.createDirectoryRecursive(outputDir)

    val decideToCopyOnDriver = getCopyFilesOnDriver(files)

    val (processedSize: Long, warnings: Seq[String]) = if (decideToCopyOnDriver) {
      var processedSize = 0L

      val warnings: Seq[String] = if (files.isEmpty) {
        log.info("Nothing to save")
        Seq.empty[String]
      } else {
        val (size, copyWarnings) = copyFilesOnDriver(files, outputDir)
        processedSize = size
        copyWarnings
      }
      (processedSize, warnings)
    } else {
      log.info("Copying files on executors...")
      if (files.isEmpty) {
        log.info("Nothing to save")
        (0L, Seq.empty[String])
      } else {
        copyFilesOnExecutors(files, outputDir)
      }
    }

    val stats = if (saveModeOpt.contains(SaveMode.Append)) {
      val list = getListOfFilesRange(infoDate, infoDate)
      if (list.isEmpty) {
        MetaTableStats(
          Option(processedSize),
          None,
          Some(processedSize),
          warnings
        )
      } else {
        val totalSize = list.map(_.getLen).sum
        MetaTableStats(
          Option(totalSize),
          Some(processedSize),
          Some(totalSize),
          warnings
        )
      }
    } else {
      MetaTableStats(
        Option(processedSize),
        None,
        Some(processedSize),
        warnings
      )
    }

    log.info(s"Stats: ${stats}")
    stats
  }

  override def getStats(infoDate: LocalDate, onlyForCurrentBatchId: Boolean): MetaTableStats = {
    val partitionDir = SparkUtils.getPartitionPath(infoDate, infoDateColumn, infoDateFormat, path)

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)

    val files = fsUtils.getHadoopFiles(partitionDir)

    var totalSize = 0L

    files.foreach(file => {
      totalSize += file.getLen
    })

    MetaTableStats(
      Option(files.length),
      None,
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

  override def isRepartitioningSupported: Boolean = false

  /**
    * Returns the list of files stored in the partition folders for all information dates in the given range.
    *
    * The range is inclusive on both ends. For each day in the range, the partition path is built from
    * the base path, the information date column and the information date format. Partition folders that
    * do not exist are skipped, since a missing partition simply means there is no data for that date.
    * If the start date is after the end date, an empty list is returned.
    *
    * @param infoDateFrom The first information date of the range (inclusive).
    * @param infoDateTo   The last information date of the range (inclusive).
    * @return The file statuses of all files found in the existing partition folders within the range,
    *         or an empty sequence if the range is empty or no partition folders exist.
    */
  private def getListOfFilesRange(infoDateFrom: LocalDate, infoDateTo: LocalDate): Seq[FileStatus] = {
    if (infoDateFrom.isAfter(infoDateTo))
      Seq.empty[FileStatus]
    else {
      val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)
      var d = infoDateFrom
      val files = mutable.ArrayBuffer.empty[FileStatus]

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

  /**
    * Returns the list of files stored directly in the base path of the metastore table.
    *
    * If the base path does not exist, an empty list is returned instead of failing.
    * Otherwise, the files located in the base path are listed using the Hadoop file system.
    */
  private def getListOfFiles: Seq[FileStatus] = {
    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)
    val hadoopPath = new Path(path)

    if (!fsUtils.exists(hadoopPath)) {
      Seq.empty[FileStatus]
    } else {
      fsUtils.getHadoopFiles(hadoopPath).toSeq
    }
  }

  /**
    * Returns the list of files stored in the partition folder for the given information date.
    *
    * The partition path is built from the base path, the information date column and the
    * information date format. If the base path exists but the partition folder does not,
    * an empty list is returned instead of failing, since a missing partition simply means
    * there is no data for that date. In all other cases, the files are listed from the
    * partition folder. An exception may be thrown if the base path itself does not exist.
    *
    * @param infoDate The information date that identifies the partition to list files from.
    * @return The file statuses of the files in the partition folder, or an empty sequence
    *         if the partition folder does not exist.
    */
  private def getListOfFiles(infoDate: LocalDate): Seq[FileStatus] = {
    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)

    val subPath = SparkUtils.getPartitionPath(infoDate, infoDateColumn, infoDateFormat, path)

    if (fsUtils.exists(new Path(path)) && !fsUtils.exists(subPath)) {
      // The absence of the partition folder means no data is there, which is okay quite often.
      // But fsUtils.getHadoopFiles() throws an exception that fails the job and dependent jobs in this case
      Seq.empty[FileStatus]
    } else {
      fsUtils.getHadoopFiles(subPath).toSeq
    }
  }

  /**
    * Converts a list of file statuses into a DataFrame describing the raw files.
    *
    * @param listOfPaths The file statuses of the raw files to include in the DataFrame.
    * @return A DataFrame with one row per file, containing the full path and the file name.
    *         Returns an empty DataFrame with the same schema if no files are provided.
    */
  private def listOfPathsToDf(listOfPaths: Seq[FileStatus]): DataFrame = {
    val list = listOfPaths.map { path =>
      (path.getPath.toString, path.getPath.getName)
    }
    if (list.isEmpty)
      getEmptyRawDf
    else {
      list.toDF(RAW_PATH_FIELD_KEY, RAW_OFFSET_FIELD_KEY)
    }
  }

  /**
    * Determines whether the raw files should be copied on the driver node or on executors.
    *
    * If copying on the driver is explicitly configured, the configured value is returned
    * as is. Otherwise, the decision is based on the number of files that need copying.
    */
  private def getCopyFilesOnDriver(files: Seq[RawFile]): Boolean = {
    copyOnDriverOpt match {
      case Some(value) => value
      case None        =>
        val needsCopyingCount = files.filter(_.needsCopying).count(_.needsCopying)
        if (needsCopyingCount < COPY_ON_EXECUTORS_MINIMAL_FILES_COUNT) {
          log.info(s"The number of files is $needsCopyingCount. Will be copied on the driver.")
          true
        } else {
          log.info(s"The number of files is $needsCopyingCount. Will be copied on executors.")
          false
        }
    }
  }

  /**
    * Copies the given raw files to the output directory sequentially on the driver node.
    *
    * The total size of every file in the list is added to the processed size,
    * regardless of whether it is copied. Only files marked with `needsCopying` are
    * copied into the output directory, keeping their original file names. Each copy
    * is performed with retries. Files that still fail to copy do not stop processing.
    * Instead, each failure is recorded as a warning.
    *
    * @param files     The list of raw files to process. Each entry holds the source file
    *                  path and a flag indicating whether the file needs to be copied.
    * @param outputDir The target directory where files requiring copying are placed.
    * @return A tuple where the first element is the total size in bytes of all source
    *         files, and the second element is the list of warning messages for files
    *         that could not be copied. Each warning is the exception message, or the
    *         exception class name if no message is available.
    */
  private def copyFilesOnDriver(files: Seq[RawFile], outputDir: Path): (Long, Seq[String]) = {
    log.info("Copying files on the driver...")
    val fsUtilsTrg = new FsUtils(spark.sparkContext.hadoopConfiguration, outputDir.toString)
    var processedSize: Long = 0
    val warnings = files.flatMap { file =>
      val srcPath = new Path(file.filePath)

      val fsSrc = srcPath.getFileSystem(spark.sparkContext.hadoopConfiguration)


      processedSize += fsSrc.getContentSummary(srcPath).getLength

      if (file.needsCopying) {
        val trgPath = new Path(outputDir, srcPath.getName)

        log.info(s"Copying file from $srcPath to $trgPath")

        fsUtilsTrg.copyFileWithRetry(srcPath, trgPath) match {
          case None => Seq.empty[String]
          case Some(ex) => Seq(Option(ex.getMessage).getOrElse(ex.getClass.getName))
        }
      } else {
        Seq.empty[String]
      }
    }
    (processedSize, warnings)
  }

  private def getEmptyRawDf(implicit spark: SparkSession): DataFrame = {
    val schema = StructType(Seq(StructField(RAW_PATH_FIELD_KEY, StringType), StructField(RAW_OFFSET_FIELD_KEY, StringType)))

    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    spark.createDataFrame(emptyRDD, schema)
  }
}

object MetastorePersistenceRaw {
  private val log = LoggerFactory.getLogger(this.getClass)

  val MEGABYTE: Long = 1024L * 1024L
  val RAW_PATH_FIELD_KEY = "path"
  val RAW_COPY_FIELD_KEY = "copy"
  val RAW_OFFSET_FIELD_KEY = "file_name"
  val COPY_ON_EXECUTORS_MINIMAL_FILE_SIZE: Long = 1000L * MEGABYTE
  val COPY_ON_EXECUTORS_MINIMAL_FILES_COUNT: Long = 5

  /**
    * Processes a collection of raw files in parallel on Spark executors. Each file's size is determined and,
    * if required, the file is copied into the output directory.
    *
    * Each file is handled by its own Spark task: the number of partitions equals the number of files.
    * Only primitive values (file path and copy flag) are captured in the closure, so nothing
    * non-serializable is shipped to executors. The Hadoop configuration is sent to executors as a broadcast
    * variable, which is destroyed once processing finishes, whether or not it succeeds.
    *
    * Copy failures do not stop the job. They are collected and returned as error messages.
    *
    * @param files     The raw files to process. Each file specifies its path and whether it needs to be copied.
    * @param outputDir The target directory that files marked for copying are copied into.
    * @param spark     The implicit Spark session used to distribute the work across executors.
    * @return A tuple of the total size in bytes of all processed source files and the combined error messages
    *         from failed copy operations. The sequence is empty if all copies succeeded.
    */
  private def copyFilesOnExecutors(files: Seq[RawFile], outputDir: Path)(implicit spark: SparkSession): (Long, Seq[String]) = {
    val filesToProcess: Seq[(String, Boolean)] = files.map(file => (file.filePath, file.needsCopying)).toSeq

    val outputDirStr = outputDir.toString
    val hadoopConfBroadcast = spark.sparkContext.broadcast(new SerializableWritable(spark.sparkContext.hadoopConfiguration))

    log.info(s"Processing ${filesToProcess.length} raw file(s) on executors (one file per task)...")

    val results: Seq[(Long, Seq[String])] = try {
      spark.sparkContext
        .parallelize(filesToProcess, filesToProcess.length)
        .map { case (filePath, needsCopying) =>
          processRawFile(filePath, needsCopying, outputDirStr, hadoopConfBroadcast.value.value)
        }
        .collect()
        .toSeq
    } finally {
      hadoopConfBroadcast.destroy()
    }

    (results.map(_._1).sum, results.flatMap(_._2))
  }

  /**
    * Processes a single raw file by determining its size and, optionally, copying it to the output directory.
    *
    * The size of the source file is always computed using the file system associated with its path.
    * If copying is requested, the file is copied (with retries) into the output directory, keeping its
    * original file name. Any failure during copying is not thrown but instead reported in the returned
    * list of error messages.
    *
    * The method is defined in the companion object because the intention is for it to be ran on executors from
    * within RDDs. Putting it here minimizes the risk of unnecessary sertialization.
    *
    * @param filePath     The full path to the source raw file.
    * @param needsCopying Whether the file should be copied to the output directory.
    * @param outputDirStr The path of the target directory the file should be copied to.
    * @param hadoopConf   The Hadoop configuration used to access the source and target file systems.
    * @return A tuple containing the size of the source file in bytes and a sequence of error messages.
    *         The sequence is empty if no copying was needed or if the copy succeeded, and contains the
    *         exception message (or the exception class name if the message is absent) if the copy failed.
    */
  private def processRawFile(filePath: String, needsCopying: Boolean, outputDirStr: String, hadoopConf: Configuration): (Long, Seq[String]) = {
    val srcPath = new Path(filePath)
    val fsSrc = srcPath.getFileSystem(hadoopConf)

    val processedSize = fsSrc.getContentSummary(srcPath).getLength

    if (needsCopying) {
      val fsUtilsTrg = new FsUtils(hadoopConf, outputDirStr)
      val trgPath = new Path(outputDirStr, srcPath.getName)

      log.info(s"Copying file from $srcPath to $trgPath")

      fsUtilsTrg.copyFileWithRetry(srcPath, trgPath) match {
        case None     => (processedSize, Seq.empty[String])
        case Some(ex) => (processedSize, Seq(Option(ex.getMessage).getOrElse(ex.getClass.getName)))
      }
    } else {
      (processedSize, Seq.empty)
    }
  }
}
