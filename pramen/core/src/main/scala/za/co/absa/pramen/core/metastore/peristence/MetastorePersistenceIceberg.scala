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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{CatalogTable, PartitionInfo, PartitionScheme}
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.utils.hive.QueryExecutor

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class MetastorePersistenceIceberg(table: CatalogTable,
                                  location: Option[String],
                                  infoDateColumn: String,
                                  infoDateFormat: String,
                                  batchIdColumn: String,
                                  batchId: Long,
                                  partitionScheme: PartitionScheme,
                                  partitionInfo: PartitionInfo,
                                  saveModeOpt: Option[SaveMode],
                                  writeOptions: Map[String, String],
                                  tableProperties: Map[String, String]
                                 )(implicit spark: SparkSession) extends MetastorePersistence {

  import MetastorePersistenceIceberg._

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    spark.table(table.getFullTableName)
      .filter(getFilter(infoDateFrom, infoDateTo))
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    ???
  }

  override def getStats(infoDate: LocalDate, onlyForCurrentBatchId: Boolean): MetaTableStats = {
    val df = loadTable(Option(infoDate), Option(infoDate))

    if (onlyForCurrentBatchId && df.schema.exists(_.name.equalsIgnoreCase(batchIdColumn))) {
      val batchCount = df.filter(col(batchIdColumn) === batchId).count()
      val countAll = df.count()

      MetaTableStats(Option(countAll), Option(batchCount), None)
    } else {
      val countAll = df.count()

      MetaTableStats(Option(countAll), None, None)
    }
  }

  override def createOrUpdateHiveTable(infoDate: LocalDate,
                                       hiveTableName: String,
                                       queryExecutor: QueryExecutor,
                                       hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Iceberg only operates on tables in a catalog. Separate Hive options are not supported.")
  }

  override def repairHiveTable(hiveTableName: String,
                               queryExecutor: QueryExecutor,
                               hiveConfig: HiveConfig): Unit = {
    throw new UnsupportedOperationException("Iceberg only operates on tables in a catalog. Separate Hive options are not supported.")
  }

  def getFilter(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): Column = {
    (infoDateFrom, infoDateTo) match {
      case (None, None) => log.warn(s"Reading '${table.getFullTableName}' without filters. This can have performance impact."); lit(true)
      case (Some(from), None) => col(infoDateColumn) >= lit(dateFormatter.format(from))
      case (None, Some(to)) => col(infoDateColumn) <= lit(dateFormatter.format(to))
      case (Some(from), Some(to)) => col(infoDateColumn) >= lit(dateFormatter.format(from)) && col(infoDateColumn) <= lit(dateFormatter.format(to))
    }
  }
}

object MetastorePersistenceIceberg {

}
