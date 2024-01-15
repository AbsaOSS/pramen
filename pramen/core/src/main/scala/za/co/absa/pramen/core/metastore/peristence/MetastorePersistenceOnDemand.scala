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

class MetastorePersistenceOnDemand(tempPath: String,
                                    tableName: String,
                                    cachePolicy: CachePolicy
                                   )(implicit spark: SparkSession) extends MetastorePersistence {

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = ???

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = ???

  override def getStats(infoDate: LocalDate): MetaTableStats = {
    throw new UnsupportedOperationException("On demand format does not support getting record count and size statistics.")
  }

  override def createOrUpdateHiveTable(infoDate: LocalDate, hiveTableName: String, queryExecutor: QueryExecutor, hiveConfig: HiveConfig): Unit = ???

  override def repairHiveTable(hiveTableName: String, queryExecutor: QueryExecutor, hiveConfig: HiveConfig): Unit = ???
}

object MetastorePersistenceOnDemand {

}
