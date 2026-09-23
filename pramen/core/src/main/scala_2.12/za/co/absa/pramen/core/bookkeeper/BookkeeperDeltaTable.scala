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

package za.co.absa.pramen.core.bookkeeper

import io.delta.tables.DeltaTable
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import za.co.absa.pramen.core.bookkeeper.model.TableSchemaJson
import za.co.absa.pramen.core.model.{DataChunk, TableSchema}

import java.time.{Instant, LocalDate}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe

object BookkeeperDeltaTable {
  val recordsTable = "bookkeeping"
  val schemasTable = "schemas"

  def getFullTableName(databaseOpt: Option[String], tablePrefix: String, tableName: String): String = {
    databaseOpt match {
      case Some(db) => s"$db.$tablePrefix$tableName"
      case None     => s"$tablePrefix$tableName"
    }
  }
}

class BookkeeperDeltaTable(database: Option[String],
                           tablePrefix: String,
                           batchId: Long)
                          (implicit spark: SparkSession) extends BookkeeperDeltaBase(batchId) {
  import BookkeeperDeltaTable._
  import spark.implicits._

  private val recordsFullTableName = getFullTableName(database, tablePrefix, recordsTable)
  private val schemasFullTableName = getFullTableName(database, tablePrefix, schemasTable)

  init()

  override def getBkDf(filter: Column): Dataset[DataChunk] = {
    val df = try {
      spark.table(recordsFullTableName).as[DataChunk]
    } catch {
      case ex: AnalysisException if ex.getMessage().contains("cannot resolve") || ex.getMessage().contains("does not exist") =>
        // Spark 2 and 3
        migrateModel()
        spark.table(recordsFullTableName).as[DataChunk]

      case ex: Throwable if ex.getMessage.contains("UNRESOLVED_COLUMN") =>
        // Spark 3 and 4
        migrateModel()
        spark.table(recordsFullTableName).as[DataChunk]
    }

    df.filter(filter)
      .orderBy(col("jobFinished"))
      .as[DataChunk]
  }

  override def saveRecordCountDelta(dataChunk: DataChunk): Unit = {
    val df = Seq(dataChunk).toDF()

    df.write
      .format("delta")
      .mode(SaveMode.Append)
      .option("mergeSchema", "true")
      .saveAsTable(recordsFullTableName)
  }

  override def deleteNonCurrentBatchRecords(table: String, infoDate: LocalDate): Unit = {
    val infoDateStr = DataChunk.dateFormatter.format(infoDate)
    val filter = (col("tableName") === lit(table)) && (col("infoDate") === lit(infoDateStr)) && (col("batchId") =!= lit(batchId))

    val deltaTable = DeltaTable.forName(spark, recordsFullTableName)
    deltaTable.delete(filter)
  }

  override def getSchemasDeltaDf: Dataset[TableSchemaJson] = {
    spark.table(schemasFullTableName).as[TableSchemaJson]
  }

  override def saveSchemaDelta(schema: TableSchema): Unit = {
    val df = Seq(
      TableSchemaJson(schema.tableName, schema.infoDate, schema.schemaJson, Instant.now().toEpochMilli)
    ).toDF()

    df.write
      .format("delta")
      .mode(SaveMode.Append)
      .option("mergeSchema", "true")
      .saveAsTable(schemasFullTableName)
  }

  override def writeEmptyDataset[T <: Product : universe.TypeTag : ClassTag](pathOrTable: String): Unit = {
    val df = Seq.empty[T].toDS

    df.write
      .format("delta")
      .saveAsTable(pathOrTable)
  }

  override def deleteTable(tableWithWildcard: String): Seq[String] = ???

  def init(): Unit = {
    initRecordsDirectory()
    initSchemasDirectory()
  }

  private def initRecordsDirectory(): Unit = {
    if (!spark.catalog.tableExists(recordsFullTableName)) {
      writeEmptyDataset[DataChunk](recordsFullTableName)
    }
  }

  private def initSchemasDirectory(): Unit = {
    if (!spark.catalog.tableExists(schemasFullTableName)) {
      writeEmptyDataset[TableSchemaJson](schemasFullTableName)
    }
  }

  private def migrateModel(): Unit = {
    migrateModelViaEmptyDataset[DataChunk](recordsFullTableName)
  }

  private def migrateModelViaEmptyDataset[T <: Product : universe.TypeTag : ClassTag](pathOrTable: String): Unit = {
    val df = Seq.empty[T].toDS

    df.write
      .format("delta")
      .mode(SaveMode.Append)
      .option("mergeSchema", "true")
      .saveAsTable(pathOrTable)
  }
}
