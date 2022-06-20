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

package za.co.absa.pramen.builtin

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.builtin.model.JdbcToParquetSyncTable
import za.co.absa.pramen.config.ConfigKeys

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class SyncJob(jobName: String,
              reader: TableReader,
              writer: TableWriter,
              tablesToSync: List[JdbcToParquetSyncTable])
             (implicit spark: SparkSession, conf: Config) extends SourceJob {

  override def name: String = jobName

  override def getReader(tableName: String): TableReader = reader

  override def getWriter(tableName: String): TableWriter = writer

  override def getTables: Seq[String] = tablesToSync.map(_.nameInSyncWatcher)
}

object SyncJob extends JobFactory[SyncJob] {
  override def apply(conf: Config, spark: SparkSession): SyncJob = {

    val syncJobName = conf.getString(ConfigKeys.SYNC_JOB_NAME)
    val syncTables = getTables(conf)

    // ToDo load reader and writer from factory
    new SyncJob(syncJobName, null, null, syncTables)(spark, conf)
  }

  private def getTables(conf: Config): List[JdbcToParquetSyncTable] = {
    var i = 1
    val syncTables = new ListBuffer[JdbcToParquetSyncTable]
    while (conf.hasPath(s"${ConfigKeys.SYNC_TABLE_PREFIX}.$i")) {
      syncTables += getSyncTable(conf, i)
      i += 1
    }
    if (syncTables.isEmpty) {
      throw new IllegalArgumentException("No tables defined for synchronization. " +
        s"Please set ${ConfigKeys.SYNC_TABLE_PREFIX}.1, etc.")
    }
    syncTables.toList
  }

  private def getSyncTable(conf: Config, n: Int): JdbcToParquetSyncTable = {
    val tableDefinition = conf.getStringList(s"${ConfigKeys.SYNC_TABLE_PREFIX}.$n").asScala.toArray

    if (tableDefinition.length != 3) {
      val lstStr = tableDefinition.mkString(", ")
      throw new IllegalArgumentException(s"Table definition for table $n is invalid. " +
        s"Expected: name, path, records per partitions. Got $lstStr")
    } else {
      JdbcToParquetSyncTable.fromTableDefParam(tableDefinition(0), tableDefinition(1), tableDefinition(2).toLong)
    }
  }
}


