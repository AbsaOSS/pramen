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
import za.co.absa.pramen.core.app.config.GeneralConfig.TEMPORARY_DIRECTORY_KEY
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.utils.hive.QueryExecutor

import java.time.LocalDate

class MetastorePersistenceTransientEager(tempPathOpt: Option[String],
                                         tableName: String,
                                         cachePolicy: CachePolicy
                                   )(implicit spark: SparkSession) extends MetastorePersistence {
  import TransientTableManager._

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    (infoDateFrom, infoDateTo) match {
      case (Some(from), Some(to)) if from == to =>
        getDataForTheDate(tableName, from)
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException("Metastore 'transient' format does not support ranged queries.")
      case _ =>
        throw new IllegalArgumentException("Metastore 'transient' format requires info date for querying its contents.")
    }
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    val (dfOut, sizeBytesOpt) = cachePolicy match {
      case CachePolicy.NoCache =>
        addRawDataFrame(tableName, infoDate, df)
      case CachePolicy.Cache =>
        addCachedDataframe(tableName, infoDate, df)
      case CachePolicy.Persist =>
        val tempPath = tempPathOpt.getOrElse(throw new IllegalArgumentException(s"Transient metastore tables with persist cache policy require temporary directory to be defined at: $TEMPORARY_DIRECTORY_KEY"))
        addPersistedDataFrame(tableName, infoDate, df, tempPath)
    }

    val recordCount = numberOfRecordsEstimate match {
      case Some(n) => n
      case None => dfOut.count()
    }

    MetaTableStats(
      recordCount,
      sizeBytesOpt
    )
  }

  override def getStats(infoDate: LocalDate): MetaTableStats = {
    throw new UnsupportedOperationException("Transient format does not support getting record count and size statistics.")
  }

  override def createOrUpdateHiveTable(infoDate: LocalDate,
                                       hiveTableName: String,
                                       queryExecutor: QueryExecutor,
                                       hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Transient format does not support Hive tables.")
  }

  override def repairHiveTable(hiveTableName: String,
                               queryExecutor: QueryExecutor,
                               hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Transient format does not support Hive tables.")
  }
}
