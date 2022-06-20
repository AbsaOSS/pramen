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

package za.co.absa.pramen.builtin.utils

import com.typesafe.config.Config
import za.co.absa.pramen.builtin.model.JdbcToParquetSyncTable
import za.co.absa.pramen.config.ConfigKeys

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object JobConfigUtils {
  import ConfigKeys._

  def getTableDefs(conf: Config): List[JdbcToParquetSyncTable] = {
    var i = 1
    val syncTables = new ListBuffer[JdbcToParquetSyncTable]
    while (conf.hasPath(s"$JDBC_SYNC_TABLE_NEW_PREFIX.$i")) {
      syncTables += getSyncTableFromConfig(conf, i)
      i += 1
    }
    if (syncTables.isEmpty) {
      getTablesFromConfigArray(conf)
    } else {
      syncTables.toList
    }
  }

  private def getTablesFromConfigArray(conf: Config): List[JdbcToParquetSyncTable] = {
    var i = 1
    val syncTables = new ListBuffer[JdbcToParquetSyncTable]
    while (conf.hasPath(s"$JDBC_SYNC_TABLE_PREFIX.$i")) {
      syncTables += getSyncTableFromArray(conf, i)
      i += 1
    }
    if (syncTables.isEmpty) {
      throw new IllegalArgumentException("No tables defined for synchronization. " +
        s"Please set $JDBC_SYNC_TABLE_NEW_PREFIX.1, etc.")
    }
    syncTables.toList
  }

  private def getSyncTableFromArray(conf: Config, n: Int): JdbcToParquetSyncTable = {
    val tableDefinition = conf.getStringList(s"$JDBC_SYNC_TABLE_PREFIX.$n").asScala.toArray

    if (tableDefinition.length != 3) {
      val lstStr = tableDefinition.mkString(", ")
      throw new IllegalArgumentException(s"Table definition for table $n is invalid. " +
        s"Expected: name, path, records per partitions. Got $lstStr")
    } else {
      JdbcToParquetSyncTable.fromTableDefParam(tableDefinition(0), tableDefinition(1), tableDefinition(2).toLong)
    }
  }

  private def getSyncTableFromConfig(conf: Config, n: Int): JdbcToParquetSyncTable = {
    val path = s"$JDBC_SYNC_TABLE_NEW_PREFIX.$n"
    val tableConfig = conf.getConfig(path)

    JdbcToParquetSyncTable.load(tableConfig, path)
  }
}
