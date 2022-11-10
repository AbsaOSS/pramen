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

package za.co.absa.pramen.core.sink

import com.typesafe.config.Config
import za.co.absa.pramen.core.utils.ConfigUtils

case class CsvConversionParams(
                                csvOptions: Map[String, String],
                                fileNamePattern: String,
                                charsetOpt: Option[String],
                                tempHadoopPath: String,
                                dateFormat: String,
                                timestampFormat: String,
                                fileNameTimestampPattern: String,
                                createEmptyCsv: Boolean,
                                columnNameTransform: ColumnNameTransform
                              )

object CsvConversionParams {
  val TEMP_HADOOP_PATH_KEY = "temp.hadoop.path"
  val FILE_NAME_PATTERN_KEY = "file.name.pattern"
  val FILE_NAME_TIMESTAMP_PATTERN_KEY = "file.name.timestamp.pattern"
  val CHARSET_KEY = "option.encoding"
  val CREATE_EMPTY_CSV_KEY = "create.empty.csv"

  val COLUMN_NAME_TRANSFORM = "column.name.transform"
  val DATE_FORMAT_KEY = "date.format"
  val TIMESTAMP_FORMAT_KEY = "timestamp.format"

  val DEFAULT_FILE_NAME_PATTERN = "@tableName_@infoDate_@timestamp"
  val DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
  val DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss Z"
  val DEFAULT_FILE_NAME_TIMESTAMP_PATTERN = "yyyyMMdd_HHmmss"

  def fromConfig(conf: Config, path: String = ""): CsvConversionParams = {
    ConfigUtils.validatePathsExistence(conf, path, Seq(TEMP_HADOOP_PATH_KEY))

    val tempHadoopPath = conf.getString(TEMP_HADOOP_PATH_KEY)

    val fileNamePattern = ConfigUtils.getOptionString(conf, FILE_NAME_PATTERN_KEY).getOrElse(DEFAULT_FILE_NAME_PATTERN)
    val fileNameTimestampPattern = ConfigUtils.getOptionString(conf, FILE_NAME_TIMESTAMP_PATTERN_KEY).getOrElse(DEFAULT_FILE_NAME_TIMESTAMP_PATTERN)
    val charset = ConfigUtils.getOptionString(conf, CHARSET_KEY)
    val createEmptyCsv = ConfigUtils.getOptionBoolean(conf, CREATE_EMPTY_CSV_KEY).getOrElse(false)

    val columnNameTransformStr = ConfigUtils.getOptionString(conf, COLUMN_NAME_TRANSFORM).getOrElse("")
    val columnNameTransform = ColumnNameTransform.fromString(columnNameTransformStr)
    val dateFormat = ConfigUtils.getOptionString(conf, DATE_FORMAT_KEY).getOrElse(DEFAULT_DATE_FORMAT)
    val timestampFormat = ConfigUtils.getOptionString(conf, TIMESTAMP_FORMAT_KEY).getOrElse(DEFAULT_DATETIME_FORMAT)

    val options = ConfigUtils.getExtraOptions(conf, "option")

    CsvConversionParams(options, fileNamePattern, charset, tempHadoopPath, dateFormat, timestampFormat, fileNameTimestampPattern, createEmptyCsv, columnNameTransform)
  }
}
