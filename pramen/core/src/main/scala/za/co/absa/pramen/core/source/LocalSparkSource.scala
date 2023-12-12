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
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.core.utils.traverser.{FsTraverser, FsTraverserLocal}
import za.co.absa.pramen.core.utils.{ConfigUtils, FsUtils}

import java.time.LocalDate

class LocalSparkSource(sparkSource: SparkSource,
                       sourceConfig: Config,
                       val hadoopTempPath: String,
                       val fileNamePattern: String,
                       val isRecursive: Boolean)(implicit spark: SparkSession) extends Source {
  private val log = LoggerFactory.getLogger(this.getClass)

  private var isConnected = false
  private var tempBasePath: HadoopPath = _
  private var fsUtils: FsUtils = _
  private val traverser: FsTraverser = new FsTraverserLocal()

  override def hasInfoDateColumn(query: Query): Boolean = false

  @throws[Exception]
  override def connect(): Unit = {
    isConnected = true
    fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, hadoopTempPath)
    tempBasePath = fsUtils.getTempPath(new HadoopPath(hadoopTempPath))
  }

  @throws[Exception]
  override def close(): Unit = {
    log.info(s"Cleaning up $tempBasePath...")
    fsUtils.deleteDirectoryRecursively(tempBasePath)
    isConnected = false
  }

  override val config: Config = sourceConfig

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val reader = getReader(query)

    reader.getRecordCount(query, infoDateBegin, infoDateEnd)
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult = {
    val reader = getReader(query)

    val df = reader.getData(query, infoDateBegin, infoDateEnd, columns)

    SourceResult(df)
  }

  def getReader(query: Query): TableReader = {
    if (!isConnected) {
      isConnected = true
      connect()
    }

    if (!query.isInstanceOf[Query.Path]) {
      throw new IllegalArgumentException(s"Query '$query' is not supported. Please, specify 'input.path' instead.")
    }

    val path = query.asInstanceOf[Query.Path]

    val hadoopPathQuery = Query.Path(copyFilesToTempHadoopDir(path.path))

    sparkSource.getReader(hadoopPathQuery)
  }

  private [core] def copyFilesToTempHadoopDir(localPath: String): String = {
    val tempPath = fsUtils.getTempPath(tempBasePath)

    traverser.traverse(localPath, fileNamePattern, isRecursive, includeHiddenFiles = false) { fileStatus =>
      val srcFile = new HadoopPath(fileStatus.path.getAbsolutePath)
      val targetFile = new HadoopPath(tempPath, fileStatus.path.getName)
      log.info(s"Copying from local $srcFile to $targetFile...")
      fsUtils.copyFromLocal(srcFile, targetFile)
    }

    tempPath.toString
  }
}

object LocalSparkSource extends ExternalChannelFactoryV2[LocalSparkSource] {
  val TEMP_HADOOP_PATH_KEY = "temp.hadoop.path"
  val FILE_NAME_PATTERN_KEY = "file.name.pattern"
  val RECURSIVE_KEY = "recursive"
  val DEFAULT_FILE_NAME_PATTERN = "*"

  override def apply(conf: Config, workflowConfig: Config, parentPath: String, spark: SparkSession): LocalSparkSource = {
    ConfigUtils.validatePathsExistence(conf, parentPath, Seq(TEMP_HADOOP_PATH_KEY))

    val tempHadoopPath = conf.getString(TEMP_HADOOP_PATH_KEY)
    val fileNamePattern = ConfigUtils.getOptionString(conf, FILE_NAME_PATTERN_KEY).getOrElse(DEFAULT_FILE_NAME_PATTERN)
    val isRecursive = ConfigUtils.getOptionBoolean(conf, RECURSIVE_KEY).getOrElse(false)

    val sparkSource = SparkSource(conf, parentPath, spark)

    new LocalSparkSource(sparkSource, conf, tempHadoopPath, fileNamePattern, isRecursive)(spark)
  }
}
