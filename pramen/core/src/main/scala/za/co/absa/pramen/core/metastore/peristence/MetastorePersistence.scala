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

package za.co.absa.pramen.core.metastore.peristence

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import za.co.absa.pramen.api.DataFormat
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.{HiveConfig, MetaTable}
import za.co.absa.pramen.core.utils.hive.QueryExecutor

import java.time.LocalDate

trait MetastorePersistence {
  def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame

  def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats

  def getStats(infoDate: LocalDate, onlyForCurrentBatchId: Boolean): MetaTableStats

  def createOrUpdateHiveTable(infoDate: LocalDate,
                              hiveTableName: String,
                              queryExecutor: QueryExecutor,
                              hiveConfig: HiveConfig): Unit

  def repairHiveTable(hiveTableName: String,
                      queryExecutor: QueryExecutor,
                      hiveConfig: HiveConfig): Unit
}

object MetastorePersistence {
  def fromMetaTable(metaTable: MetaTable, conf: Config, saveModeOverride: Option[SaveMode] = None, batchId: Long)(implicit spark: SparkSession): MetastorePersistence = {
    val saveModeOpt = saveModeOverride.orElse(metaTable.saveModeOpt)

    metaTable.format match {
      case DataFormat.Parquet(path, recordsPerPartition) =>
        new MetastorePersistenceParquet(
          path, metaTable.infoDateColumn, metaTable.infoDateFormat, metaTable.batchIdColumn, batchId, recordsPerPartition, saveModeOpt, metaTable.readOptions, metaTable.writeOptions
        )
      case DataFormat.Delta(query, recordsPerPartition)  =>
        new MetastorePersistenceDelta(
          query, metaTable.infoDateColumn, metaTable.infoDateFormat, metaTable.batchIdColumn, batchId, metaTable.partitionByInfoDate, recordsPerPartition, saveModeOpt, metaTable.readOptions, metaTable.writeOptions
        )
      case DataFormat.Raw(path)                          =>
        new MetastorePersistenceRaw(path, metaTable.infoDateColumn, metaTable.infoDateFormat, saveModeOpt)
      case DataFormat.TransientEager(cachePolicy)             =>
        new MetastorePersistenceTransientEager(TransientTableManager.getTempDirectory(cachePolicy, conf), metaTable.name, cachePolicy)
      case DataFormat.Transient(cachePolicy) =>
        new MetastorePersistenceTransient(TransientTableManager.getTempDirectory(cachePolicy, conf), metaTable.name, cachePolicy)
      case DataFormat.Null() =>
        throw new UnsupportedOperationException(s"The metatable '${metaTable.name}' does not support writes.")
    }
  }
}
