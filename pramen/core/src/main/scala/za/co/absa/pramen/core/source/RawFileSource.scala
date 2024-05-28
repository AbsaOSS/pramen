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

package za.co.absa.pramen.core.source

import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.core.utils.{ConfigUtils, FsUtils}

import java.io.FileNotFoundException
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.matching.Regex

/**
  * This source allows loading files, and copying them ot the metastore without looking into its contents.
  *
  * In order to store files in the metastore without looking into its contents, you need to use 'raw' format of
  * the output table in the metastore. When you query such table, it returns the list of files available rather than
  * data in these files.
  *
  * For the full example see this interation test: `FileSourcingSuite`.
  *
  * Example source definition:
  * {{{
  *  {
  *    name = "file_source"
  *    factory.class = "za.co.absa.pramen.core.source.RawFileSource"
  *  }
  * }}}
  *
  * Metastore definition:
  * {{{
  *   pramen.metastore {
  *   tables = [
  *     {
  *       name = "table1"
  *       description = "Table 1 (file based)"
  *       format = "raw"
  *       path = /bigdata/metastore/table1
  *     }
  *   ]
  * }}}
  *
  * Here is an example of an ingestion operation for this source.
  *
  * {{{
  *  {
  *    name = "Sourcing from a folder"
  *    type = "ingestion"
  *    schedule.type = "daily"
  *
  *    source = "file_source"
  *
  *    tables = [
  *      {
  *        input.path = /bigdata/landing/FILE_{{yyyy-MM-dd}}*
  *        output.metastore.table = table1
  *      }
  *    ]
  *  }
  * }}}
  *
  */
class RawFileSource(val sourceConfig: Config,
                    val options: Map[String, String])(implicit spark: SparkSession) extends Source {

  import RawFileSource._
  import spark.implicits._

  private[core] val caseSensitivePattern = ConfigUtils.getOptionBoolean(sourceConfig, FILE_PATTERN_CASE_SENSITIVE_KEY).getOrElse(true)

  override val config: Config = sourceConfig

  override def hasInfoDateColumn(query: Query): Boolean = {
    query match {
      case Query.Path(pathPattern) => pathPattern.contains("{{")
      case _: Query.Custom => false
      case _ => throw new IllegalArgumentException("RawFileSource only supports 'path' or 'file.1,...' as an input, 'sql' and 'table' are not supported.")
    }
  }

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    getPaths(query, infoDateBegin, infoDateEnd)
      .map(_.getLen)
      .sum
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult = {
    val files = getPaths(query, infoDateBegin, infoDateEnd)
    val df = files.map(_.getPath.toString).toDF(PATH_FIELD)
    val fileNames = files.map(_.getPath.getName).sorted

    SourceResult(df, fileNames)
  }

  @throws[FileNotFoundException]
  private[source] def getPaths(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Seq[FileStatus] = {
    query match {
      case Query.Path(pathPattern) => getPatternBasedFilesForRange(pathPattern, infoDateBegin, infoDateEnd)
      case Query.Custom(options) => getMultiList(options)
      case _ => throw new IllegalArgumentException("RawFileSource only supports 'path' or 'file.1,...' as an input, 'sql' and 'table' are not supported.")
    }
  }

  @throws[FileNotFoundException]
  private[source] def getPatternBasedFilesForRange(pathPattern: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Seq[FileStatus] = {
    if (!pathPattern.contains("{{") || infoDateBegin.isEqual(infoDateEnd)) {
      getListOfFilesForPathPattern(pathPattern, infoDateBegin, caseSensitivePattern)
    } else {
      if (infoDateBegin.isAfter(infoDateEnd)) {
        throw new IllegalArgumentException(s"Begin date is more recent than the end date: $infoDateBegin > $infoDateEnd.")
      }
      val files = new ListBuffer[FileStatus]
      var date = infoDateBegin
      while (date.isBefore(infoDateEnd) || date.isEqual(infoDateEnd)) {
        files ++= getListOfFilesForPathPattern(pathPattern, date, caseSensitivePattern)
        date = date.plusDays(1)
      }
      files.toSeq
    }
  }
}

object RawFileSource extends ExternalChannelFactory[RawFileSource] {
  private val log = LoggerFactory.getLogger(this.getClass)

  val PATH_FIELD = "path"
  val FILE_PREFIX = "file"
  val FILE_PATTERN_CASE_SENSITIVE_KEY = "file.pattern.case.sensitive"

  val datePatternRegExp: Regex = ".*\\{\\{(\\S+)\\}\\}.*".r

  override def apply(conf: Config, parentPath: String, spark: SparkSession): RawFileSource = {
    val options = ConfigUtils.getExtraOptions(conf, "option")

    new RawFileSource(conf, options)(spark)
  }

  private[core] def getMultiList(options: Map[String, String])(implicit spark: SparkSession): Seq[FileStatus] = {
    var i = 1
    val files = new ListBuffer[FileStatus]
    var fs: FileSystem = null

    while (options.contains(s"$FILE_PREFIX.$i")) {
      val filePath = new Path(options(s"$FILE_PREFIX.$i"))

      if (fs == null) {
        fs = filePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
      }

      files += fs.getFileStatus(filePath)

      i += 1
    }
    files.toSeq
  }

  @throws[FileNotFoundException]
  private[core] def getListOfFilesForPathPattern(pathPattern: String,
                                                 infoDate: LocalDate,
                                                 caseSensitive: Boolean)
                                                (implicit spark: SparkSession): Seq[FileStatus] = {
    val globPattern = getGlobPattern(pathPattern, infoDate)
    log.info(s"Using the following pattern for '$infoDate': $globPattern")

    getListOfFiles(globPattern, caseSensitive)
  }


  @throws[FileNotFoundException]
  private[core] def getListOfFiles(pathPattern: String, caseSensitive: Boolean)(implicit spark: SparkSession): Seq[FileStatus] = {
    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, pathPattern)
    val hadoopPath = new Path(pathPattern)
    val parentPath = new Path(pathPattern).getParent

    val pathExists = {
      try {
        parentPath.isRoot || fsUtils.exists(parentPath)
      } catch {
        case NonFatal(ex) => throw new IllegalArgumentException(s"Unable to access path: $parentPath", ex)
      }
    }

    if (!pathExists) {
      throw new FileNotFoundException(s"Input path does not exist: $parentPath")
    }

    try {
      if (caseSensitive) {
        log.info(s"Using case-sensitive Hadoop file search.")
        fsUtils.getHadoopFiles(hadoopPath, includeHiddenFiles = true)
      } else {
        log.info(s"Using case-insensitive Hadoop file search.")
        fsUtils.getHadoopFilesCaseInsensitive(hadoopPath, includeHiddenFiles = true)
      }
    } catch {
      case ex: IllegalArgumentException if ex.getMessage.contains("Input path does not exist") =>
        Seq.empty[FileStatus]
      case NonFatal(ex) =>
        throw new IllegalArgumentException(s"Unable to access path: $hadoopPath", ex)
    }
  }

  private[core] def getGlobPattern(filePattern: String, infoDate: LocalDate): String = {
    filePattern match {
      case filePattern@datePatternRegExp(dateFormat) =>
        filePattern.replace(s"{{$dateFormat}}", infoDate.format(DateTimeFormatter.ofPattern(dateFormat)))
      case filePattern =>
        filePattern
    }
  }
}
