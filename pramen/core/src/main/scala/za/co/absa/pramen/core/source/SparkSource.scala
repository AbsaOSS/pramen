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
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{Query, Source, TableReader}
import za.co.absa.pramen.core.ExternalChannelFactory
import za.co.absa.pramen.core.config.Keys.KEYS_TO_REDACT
import za.co.absa.pramen.core.reader.TableReaderSpark
import za.co.absa.pramen.core.utils.ConfigUtils

class SparkSource(format: String,
                  hasInfoDateCol: Boolean,
                  infoDateColumn: String,
                  infoDateFormat: String,
                  sourceConfig: Config,
                  sourceConfigParentPath: String,
                  options: Map[String, String])(implicit spark: SparkSession) extends Source {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def hasInfoDate: Boolean = hasInfoDateCol

  override def getReader(query: Query, columns: Seq[String]): TableReader = {
    query match {
      case Query.Table(_)   =>
        throw new IllegalArgumentException(s"Unexpected 'table' spec for the Spark reader. Only 'path' is supported. Config path: $sourceConfigParentPath")
      case Query.Sql(_)     =>
        throw new IllegalArgumentException(s"Unexpected 'sql' spec for the Spark reader. Only 'path' is supported. Config path: $sourceConfigParentPath")
      case Query.Path(path) =>
        log.info(s"Using TableReaderSpark to read '$format' from: $path")
        ConfigUtils.logExtraOptions(s"Options passed for '$format':", options, KEYS_TO_REDACT)
        new TableReaderSpark(format, path, hasInfoDateCol, infoDateColumn, infoDateFormat, options)
    }
  }
}

object SparkSource extends ExternalChannelFactory[SparkSource] {
  val FORMAT = "format"
  val HAS_INFO_DATE = "has.information.date.column"
  val INFO_COLUMN_NAME = "information.date.column"
  val INFO_COLUMN_FORMAT = "information.date.app.format"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): SparkSource = {
    ConfigUtils.validatePathsExistence(conf, parentPath, Seq(FORMAT))

    val format = conf.getString(FORMAT)

    val hasInfoDate = conf.hasPath(HAS_INFO_DATE) && conf.getBoolean(HAS_INFO_DATE)
    val (infoDateColumn, infoDateFormat) = if (hasInfoDate) {
      (conf.getString(INFO_COLUMN_NAME), conf.getString(INFO_COLUMN_FORMAT))
    } else {
      ("", "")
    }

    val options = ConfigUtils.getExtraOptions(conf, "option")

    new SparkSource(format, hasInfoDate, infoDateColumn, infoDateFormat, conf, parentPath, options)(spark)
  }
}
