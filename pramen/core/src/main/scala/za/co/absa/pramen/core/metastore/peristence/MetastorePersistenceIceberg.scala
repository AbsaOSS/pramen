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
import za.co.absa.pramen.api.{CatalogTable, PartitionScheme}
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.HiveConfig
import za.co.absa.pramen.core.utils.hive.QueryExecutor

import java.sql.Date
import java.time.LocalDate

class MetastorePersistenceIceberg(table: CatalogTable,
                                  location: Option[String],
                                  description: String,
                                  infoDateColumn: String,
                                  batchIdColumn: String,
                                  batchId: Long,
                                  partitionScheme: PartitionScheme,
                                  saveModeOpt: Option[SaveMode],
                                  writeOptions: Map[String, String],
                                  tableProperties: Map[String, String]
                                 )(implicit spark: SparkSession) extends MetastorePersistence {

  import MetastorePersistenceIcebergOps._

  private val log = LoggerFactory.getLogger(this.getClass)

  override def loadTable(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): DataFrame = {
    spark.table(table.getFullTableName)
      .filter(getFilter(infoDateFrom, infoDateTo))
  }

  override def saveTable(infoDate: LocalDate, df: DataFrame, numberOfRecordsEstimate: Option[Long]): MetaTableStats = {
    val fullTableName = table.getFullTableName
    val dfToSave = addPartitionColumn(infoDate, df)

    val (isAppend, operationStr) = saveModeOpt match {
      case Some(SaveMode.Append) => (true, "Appending to")
      case _ => (false, "Writing to")
    }

    if (spark.catalog.tableExists(table.getFullTableName)) {
      log.info(s"$operationStr to table $fullTableName...")
      if (isAppend) {
        appendToTable(dfToSave, fullTableName, writeOptions)
      } else {
        overwriteDailyPartition(infoDate, dfToSave, fullTableName, infoDateColumn, writeOptions)
      }
    } else {
      log.info(s"Creating Iceberg table ${table.getFullTableName}...")
      createIcebergTable(dfToSave, fullTableName, infoDateColumn, location, description, partitionScheme, tableProperties, writeOptions)
    }

    getStats(infoDate, onlyForCurrentBatchId = false)
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
      case (Some(from), None) => col(infoDateColumn) >= lit(Date.valueOf(from))
      case (None, Some(to)) => col(infoDateColumn) <= lit(Date.valueOf(to))
      case (Some(from), Some(to)) => col(infoDateColumn) >= Date.valueOf(from) && col(infoDateColumn) <= lit(Date.valueOf(to))
    }
  }

  def addPartitionColumn(infoDate: LocalDate, df: DataFrame): DataFrame = {
    partitionScheme match {
      case PartitionScheme.NotPartitioned =>
        df.drop(infoDateColumn)
          .select(lit(Date.valueOf(infoDate)).as(infoDateColumn) +: df.columns.map(col): _*)
      case _ =>
        df.withColumn(infoDateColumn, lit(Date.valueOf(infoDate)))
    }
  }
}

object MetastorePersistenceIceberg {
  private val log = LoggerFactory.getLogger(this.getClass)

  def addGeneratedColumnPartition(table: String,
                                  infoDateColumn: String,
                                  partitionScheme: PartitionScheme)(implicit spark: SparkSession): Unit = {
    val sqls = getAddGeneratedPartitionColumnSql(table, infoDateColumn, partitionScheme)

    sqls.foreach { sql =>
      log.info(s"Executing: $sql")
      spark.sql(sql).count()
    }
  }

  def getAddGeneratedPartitionColumnSql(table: String,
                                        infoDateColumn: String,
                                        partitionScheme: PartitionScheme): Seq[String] = {
    partitionScheme match {
      case PartitionScheme.PartitionByMonth(monthColumn, yearColumn, true) =>
        Seq(
          s"""ALTER TABLE $table ADD PARTITION FIELD year($infoDateColumn) AS $yearColumn""",
          s"""ALTER TABLE $table ADD PARTITION FIELD month($infoDateColumn) AS $monthColumn"""
        )
      case PartitionScheme.PartitionByMonth(_, _, _) =>
        Seq(
          s"""ALTER TABLE $table ADD PARTITION FIELD years($infoDateColumn)""",
          s"""ALTER TABLE $table ADD PARTITION FIELD months($infoDateColumn)"""
        )
      case PartitionScheme.PartitionByYearMonth(yearMonthColumn, true) =>
        Seq(s"""ALTER TABLE $table ADD PARTITION FIELD concat(year($infoDateColumn), '_', lpad(month($infoDateColumn), 2, '0')) AS $yearMonthColumn""")
      case PartitionScheme.PartitionByYearMonth(_, _) =>
        Seq(s"""ALTER TABLE $table ADD PARTITION FIELD concat(year($infoDateColumn), '_', lpad(month($infoDateColumn), 2, '0'))""")
      case PartitionScheme.PartitionByYear(yearColumn, true) =>
        Seq(s"""ALTER TABLE $table ADD PARTITION FIELD year($infoDateColumn) AS $yearColumn""")
      case PartitionScheme.PartitionByYear(_, _) =>
        Seq(s"""ALTER TABLE $table ADD PARTITION FIELD years($infoDateColumn)""")
      case _ =>
        throw new UnsupportedOperationException(s"Partition scheme $partitionScheme is not supported for adding generated columns.")
    }
  }

}
