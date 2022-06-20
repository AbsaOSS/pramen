/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.pramen.builtin.model

import com.typesafe.config.Config
import za.co.absa.pramen.framework.utils.ConfigUtils

case class JdbcToParquetSyncTable(
                                   source: JdbcSource,
                                   nameInSyncWatcher: String,
                                   columns: Seq[String],
                                   outputPath: String,
                                   recordsPerPartition: Long
                    )
object JdbcToParquetSyncTable {
  val NAME_IN_DB = "db.table"
  val SQL_QUERY = "sql"
  val NAME_IN_SYNCWATCHER = "pramen.table"
  val TABLE_COLUMNS = "columns"
  val OUTPUT_PATH = "output.path"
  val RECORDS_PER_PARTITION = "records.per.partition"

  def load(conf: Config, parent: String = ""): JdbcToParquetSyncTable = {
    ConfigUtils.validatePathsExistence(conf,
      parent,
      OUTPUT_PATH :: RECORDS_PER_PARTITION :: Nil)

    val jdbcSource = if (conf.hasPath(SQL_QUERY)) {
      JdbcSource.Query(conf.getString(SQL_QUERY))
    } else if (conf.hasPath(NAME_IN_DB)) {
      JdbcSource.Table(conf.getString(NAME_IN_DB))
    } else {
      throw new IllegalArgumentException(s"Either $parent.$NAME_IN_DB or $parent.$SQL_QUERY should be defined.")
    }

    val tableNameInSyncWatcher = ConfigUtils.getOptionString(conf, NAME_IN_SYNCWATCHER)
      .getOrElse(conf.getString(NAME_IN_DB))

    JdbcToParquetSyncTable(
      source = jdbcSource,
      nameInSyncWatcher = tableNameInSyncWatcher,
      columns = ConfigUtils.getOptListStrings(conf, TABLE_COLUMNS),
      outputPath = conf.getString(OUTPUT_PATH),
      recordsPerPartition = conf.getLong(RECORDS_PER_PARTITION)
    )
  }

  def fromTableDefParam(name: String, path: String, recordsPerPartition: Long): JdbcToParquetSyncTable = {
    JdbcToParquetSyncTable(JdbcSource.Table(name), name, Seq.empty, path, recordsPerPartition)
  }
}