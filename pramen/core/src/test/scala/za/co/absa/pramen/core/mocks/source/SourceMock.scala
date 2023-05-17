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

package za.co.absa.pramen.core.mocks.source

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api._
import za.co.absa.pramen.core.source.{LocalSparkSource, SparkSource}
import za.co.absa.pramen.core.utils.ConfigUtils

import java.time.LocalDate

class SourceMock(
                  mockDataAlwaysAvailable: Boolean = false,
                  connectException: Option[Throwable] = None,
                  closedException: Option[Throwable] = None,
                  getRecordCountFunction: () => Long = () => 0L,
                  getDataFunction: () => SourceResult = () => null
                ) extends Source {
  var connectCalled = 0
  var closeCaller = 0
  var getRecordCountCalled = 0
  var getDataCalled = 0

  override def isDataAlwaysAvailable: Boolean = mockDataAlwaysAvailable

  @throws[Exception]
  override def connect(): Unit = {
    connectCalled += 1
    connectException.foreach(throw _)
  }

  override def close(): Unit = {
    closeCaller += 1
    closedException.foreach(throw _)
  }

  override def config: Config = null

  override def validate(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Reason = {
    Reason.Ready
  }

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    getRecordCountCalled += 1
    getRecordCountFunction()
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult = {
    getDataCalled += 1
    getDataFunction()
  }
}

object SourceMock extends ExternalChannelFactory[LocalSparkSource] {
  val TEMP_HADOOP_PATH_KEY = "temp.hadoop.path"
  val FILE_NAME_PATTERN_KEY = "file.name.pattern"
  val RECURSIVE_KEY = "recursive"
  val DEFAULT_FILE_NAME_PATTERN = "*"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): LocalSparkSource = {
    ConfigUtils.validatePathsExistence(conf, parentPath, Seq(TEMP_HADOOP_PATH_KEY))

    val tempHadoopPath = conf.getString(TEMP_HADOOP_PATH_KEY)
    val fileNamePattern = ConfigUtils.getOptionString(conf, FILE_NAME_PATTERN_KEY).getOrElse(DEFAULT_FILE_NAME_PATTERN)
    val isRecursive = ConfigUtils.getOptionBoolean(conf, RECURSIVE_KEY).getOrElse(false)

    val sparkSource = SparkSource(conf, parentPath, spark)

    new LocalSparkSource(sparkSource, conf, tempHadoopPath, fileNamePattern, isRecursive)(spark)
  }
}
