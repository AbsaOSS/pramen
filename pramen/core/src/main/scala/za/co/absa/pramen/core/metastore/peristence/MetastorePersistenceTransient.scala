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

import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.CachePolicy
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.utils.hive.QueryExecutor

import java.time.LocalDate

class MetastorePersistenceTransient(tempPath: Option[String],
                                    tableName: String,
                                    cachePolicy: CachePolicy
                                  )(implicit spark: SparkSession) extends MetastorePersistence {
  val transientPersistence = new MetastorePersistenceTransientEager(tempPath, tableName, cachePolicy)

  import TransientJobManager._

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    (infoDateFrom, infoDateTo) match {
      case (Some(from), Some(to)) if from == to =>
        runLazyTask(tableName, from)
      case (Some(from), Some(to)) =>
        runLazyTasks(tableName, selectInfoDatesToExecute(tableName, from, to))
      case (None, Some(until)) =>
        runLazyTask(tableName, selectLatestLazySnapshot(tableName, until))
      case _ =>
        throw new IllegalArgumentException("Metastore 'transient' format requires info date for querying its contents.")
    }
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    transientPersistence.saveTable(infoDate, df, numberOfRecordsEstimate)
  }

  override def getStats(infoDate: LocalDate): MetaTableStats = {
    throw new UnsupportedOperationException("The 'transient' format does not support getting record count and size statistics.")
  }

  override def createOrUpdateHiveTable(infoDate: LocalDate, hiveTableName: String, queryExecutor: QueryExecutor, hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("The 'transient' format does not support Hive tables.")
  }

  override def repairHiveTable(hiveTableName: String, queryExecutor: QueryExecutor, hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("The 'transient' format does not support Hive tables.")
  }
}
