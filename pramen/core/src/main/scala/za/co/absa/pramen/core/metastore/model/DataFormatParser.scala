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

package za.co.absa.pramen.core.metastore.model

import com.typesafe.config.Config
import za.co.absa.pramen.api.{DataFormat, Query}
import za.co.absa.pramen.core.utils.ConfigUtils

object DataFormatParser {
  val FORMAT_PARQUET = "parquet"
  val FORMAT_DELTA = "delta"
  val FORMAT_RAW = "raw"

  val FORMAT_KEY = "format"
  val PATH_KEY = "path"
  val TABLE_KEY = "table"
  val RECORDS_PER_PARTITION_KEY = "records.per.partition"
  val DEFAULT_FORMAT = "parquet"

  val DEFAULT_RECORDS_PER_PARTITION_KEY = "pramen.default.records.per.partition"

  def fromConfig(conf: Config, appConfig: Config): DataFormat = {
    val format = ConfigUtils.getOptionString(conf, FORMAT_KEY).getOrElse(DEFAULT_FORMAT)

    val defaultRecordsPerPartition = ConfigUtils.getOptionLong(appConfig, DEFAULT_RECORDS_PER_PARTITION_KEY)

    format match {
      case FORMAT_PARQUET =>
        val path = getPath(conf)
        val recordsPerPartition = ConfigUtils.getOptionLong(conf, RECORDS_PER_PARTITION_KEY)
          .orElse(defaultRecordsPerPartition)
        DataFormat.Parquet(path, recordsPerPartition)
      case FORMAT_DELTA   =>
        val query = getQuery(conf)
        val recordsPerPartition = ConfigUtils.getOptionLong(conf, RECORDS_PER_PARTITION_KEY)
          .orElse(defaultRecordsPerPartition)
        DataFormat.Delta(query, recordsPerPartition)
      case FORMAT_RAW =>
        if (!conf.hasPath(PATH_KEY)) throw new IllegalArgumentException(s"Mandatory option for a metastore table having 'raw' format: $PATH_KEY")
        val path = Query.Path(conf.getString(PATH_KEY))
        DataFormat.Raw(path)
      case _              => throw new IllegalArgumentException(s"Unknown format: $format")
    }
  }

  private def getPath(conf: Config): String = {
    ConfigUtils.getOptionString(conf, PATH_KEY).getOrElse(throw new IllegalArgumentException(s"Mandatory option missing: $PATH_KEY"))
  }

  private def getQuery(conf: Config): Query = {
    conf match {
      case conf if conf.hasPath(PATH_KEY)  => Query.Path(conf.getString(PATH_KEY))
      case conf if conf.hasPath(TABLE_KEY) => Query.Table(conf.getString(TABLE_KEY))
      case _                               => throw new IllegalArgumentException(s"Mandatory option missing: $PATH_KEY or $TABLE_KEY")
    }
  }
}
