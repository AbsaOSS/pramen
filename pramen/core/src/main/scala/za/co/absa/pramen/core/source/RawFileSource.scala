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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api._
import za.co.absa.pramen.core.utils.{ConfigUtils, FsUtils}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
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
  *        input.path = /bigdata/landing
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

  override val config: Config = sourceConfig

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    getPaths(query, infoDateBegin, infoDateEnd).length
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult = {
    val files = getPaths(query, infoDateBegin, infoDateEnd)
    val df = files.toDF(PATH_FIELD)

    SourceResult(df, files)
  }

  private[source] def getPaths(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Seq[String] = {
    query match {
      case Query.Path(pathPattern) => getPatternBasedFilesForRange(pathPattern, infoDateBegin, infoDateEnd)
      case Query.Custom(options) => getMultiList(options)
      case _ => throw new IllegalArgumentException("RawFileSource only supports 'path' or 'file.1,...' as an input, 'sql' and 'table' are not supported.")
    }
  }

  private[source] def getPatternBasedFilesForRange(pathPattern: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Seq[String] = {
    if (!pathPattern.contains("{{") || infoDateBegin.isEqual(infoDateEnd)) {
      getListOfFilesForPathPattern(pathPattern, infoDateBegin)
    } else {
      if (infoDateBegin.isAfter(infoDateEnd)) {
        throw new IllegalArgumentException(s"Begin date is more recent than the end date: $infoDateBegin > $infoDateEnd.")
      }
      val files = new ListBuffer[String]
      var date = infoDateBegin
      while (date.isBefore(infoDateEnd) || date.isEqual(infoDateEnd)) {
        files ++= getListOfFilesForPathPattern(pathPattern, infoDateBegin)
        date = date.plusDays(1)
      }
      files.toSeq
    }
  }
}

object RawFileSource extends ExternalChannelFactory[RawFileSource] {
  val PATH_FIELD = "path"
  val FILE_PREFIX = "file"

  val datePatternRegExp: Regex = ".*\\{\\{(\\S+)\\}\\}.*".r

  override def apply(conf: Config, parentPath: String, spark: SparkSession): RawFileSource = {
    val options = ConfigUtils.getExtraOptions(conf, "option")

    new RawFileSource(conf, options)(spark)
  }

  private[core] def getMultiList(options: Map[String, String]): Seq[String] = {
    var i = 1
    val files = new ListBuffer[String]

    while (options.contains(s"$FILE_PREFIX.$i")) {
      val filePath = options(s"$FILE_PREFIX.$i")

      files += filePath

      i += 1
    }
    files.toSeq
  }

  private[core] def getListOfFilesForPathPattern(pathPattern: String,
                                                 infoDate: LocalDate)
                                                (implicit spark: SparkSession): Seq[String] = {
    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, pathPattern)

    val globPattern = if (pathPattern.contains("{{"))
      getGlobPattern(pathPattern, infoDate)
    else {
      pathPattern
    }

    val hadoopPath = new Path(globPattern)
    if (fsUtils.exists(hadoopPath) && fsUtils.isDirectory(hadoopPath)) {
      fsUtils.getHadoopFiles(new Path(pathPattern), includeHiddenFiles = true).sorted
    } else {
      getListOfFiles(globPattern)
    }
  }


  private[core] def getListOfFiles(pathPattern: String)(implicit spark: SparkSession): Seq[String] = {
    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, pathPattern)
    val hadoopPath = new Path(pathPattern)

    val parentPath = new Path(pathPattern).getParent

    if (!parentPath.isRoot && !fsUtils.exists(parentPath)) {
      throw new IllegalArgumentException(s"Input path does not exist: $parentPath")
    }

    try {
      fsUtils.getHadoopFiles(hadoopPath, includeHiddenFiles = true).sorted
    } catch {
      case ex: IllegalArgumentException if ex.getMessage.contains("Input path does not exist") => Seq.empty[String]
    }
  }

  private[core] def getGlobPattern(filePattern: String, infoDate: LocalDate): String = {
    filePattern match {
      case filePattern@datePatternRegExp(dateFormat) =>
        filePattern.replace(s"{{$dateFormat}}", infoDate.format(DateTimeFormatter.ofPattern(dateFormat)))
      case filePattern =>
        throw new IllegalArgumentException(s"File pattern '$filePattern' does not contain date format in curly braces, e.g. 'FILE_{{yyyyMMdd}}.dat'.")
    }
  }
}
