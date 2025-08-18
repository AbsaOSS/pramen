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

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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

    val tableExists = doesTableExist(table)

    if (tableExists) {
      log.info(s"$operationStr to table $fullTableName...")
      if (isAppend) {
        appendToTable(dfToSave, fullTableName, writeOptions)
      } else {
        if (partitionScheme == PartitionScheme.Overwrite) {
          overwriteFullTable(dfToSave, fullTableName, writeOptions)
        } else {
          overwriteDailyPartition(infoDate, dfToSave, fullTableName, infoDateColumn, writeOptions)
        }
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

  def doesTableExist(catalogTable: CatalogTable)(implicit spark: SparkSession): Boolean = {
    try {
      spark.read.table(catalogTable.getFullTableName)
      true
    } catch {
      // This is a common error
      case ex: AnalysisException if ex.getMessage().contains("Table or view not found") || ex.getMessage().contains("TABLE_OR_VIEW_NOT_FOUND") => false
      // This is the exception, needs to be re-thrown.
      case ex: AnalysisException if ex.getMessage().contains("TableType cannot be null for table:") =>
        throw new IllegalArgumentException("Attempt to use a catalog not supported by the file format. " +
          "Ensure you are using the iceberg catalog and/or it is set as the default catalog with (spark.sql.defaultCatalog) " +
          "or the catalog is specified explicitly as the table name.", ex)
    }
  }

  def getFilter(infoDateFrom: Option[LocalDate], infoDateTo: Option[LocalDate]): Column = {
    if (partitionScheme == PartitionScheme.Overwrite) {
      if (infoDateFrom.isDefined || infoDateTo.isDefined) {
        log.warn(s"Date filter is ignored when the partition scheme is 'Overwrite' for '${table.getFullTableName}'.")
      }
      lit(true)
    } else {
      (infoDateFrom, infoDateTo) match {
        case (None, None) => log.warn(s"Reading '${table.getFullTableName}' without filters. This can have performance impact."); lit(true)
        case (Some(from), None) => col(infoDateColumn) >= lit(Date.valueOf(from))
        case (None, Some(to)) => col(infoDateColumn) <= lit(Date.valueOf(to))
        case (Some(from), Some(to)) => col(infoDateColumn) >= lit(Date.valueOf(from)) && col(infoDateColumn) <= lit(Date.valueOf(to))
      }
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
      case PartitionScheme.PartitionByMonth(_, _) =>
        Seq(
          s"""ALTER TABLE $table ADD PARTITION FIELD year($infoDateColumn)""",
          s"""ALTER TABLE $table ADD PARTITION FIELD month($infoDateColumn)"""
        )
      case PartitionScheme.PartitionByYearMonth(_) =>
        throw new UnsupportedOperationException(s"Partition scheme $partitionScheme is not supported by Iceberg.")
      case PartitionScheme.PartitionByYear(_) =>
        Seq(s"""ALTER TABLE $table ADD PARTITION FIELD year($infoDateColumn)""")
      case _ =>
        throw new UnsupportedOperationException(s"Partition scheme $partitionScheme is not supported for adding generated columns.")
    }
  }

}
