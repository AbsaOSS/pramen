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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, SaveMode, SparkSession}
import za.co.absa.pramen.core.bookkeeper.model.TableSchemaJson
import za.co.absa.pramen.core.model.{DataChunk, TableSchema}
import za.co.absa.pramen.core.utils.FsUtils

import java.time.Instant
import scala.reflect.ClassTag
import scala.reflect.runtime.universe

object BookkeeperDeltaPath {
  val bookkeepingRootPath = "bk"
  val recordsDirName = "records_delta"
  val schemasDirName = "schemas_delta"
  val locksDirName = "locks"
}

class BookkeeperDeltaPath(bookkeepingPath: String)(implicit spark: SparkSession) extends BookkeeperDeltaBase {
  import BookkeeperDeltaPath._
  import spark.implicits._

  private val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, bookkeepingPath)
  private val bkPath = new Path(bookkeepingPath, bookkeepingRootPath)
  private val recordsPath = new Path(bkPath, recordsDirName)
  private val schemasPath = new Path(bkPath, schemasDirName)
  private val locksPath = new Path(bookkeepingPath, locksDirName)

  init()

  override def getBkDf(filter: Column): Dataset[DataChunk] = {
    val df = spark
      .read
      .format("delta")
      .load(recordsPath.toUri.toString)

    df.filter(filter)
      .orderBy(col("jobFinished"))
      .as[DataChunk]
  }

  override def saveRecordCountDelta(dataChunk: DataChunk): Unit = {
    val df = Seq(dataChunk).toDF()

    df.write
      .mode(SaveMode.Append)
      .format("delta")
      .option("mergeSchema", "true")
      .save(recordsPath.toUri.toString)
  }

  override def getSchemasDeltaDf: Dataset[TableSchemaJson] = {
    spark
      .read
      .format("delta")
      .load(schemasPath.toUri.toString)
      .as[TableSchemaJson]
  }

  override def saveSchemaDelta(schema: TableSchema): Unit = {
    val df = Seq(
      TableSchemaJson(schema.tableName, schema.infoDate, schema.schemaJson, Instant.now().toEpochMilli)
    ).toDF()

    df.write
      .mode(SaveMode.Append)
      .format("delta")
      .option("mergeSchema", "true")
      .save(schemasPath.toUri.toString)
  }

  override def writeEmptyDataset[T <: Product : universe.TypeTag : ClassTag](pathOrTable: String): Unit = {
    val df = Seq.empty[T].toDS

    df.write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .save(pathOrTable)
  }

  private def init(): Unit = {
    initRecordsDirectory(recordsPath)
    initSchemasDirectory(schemasPath)
    initDirectory(locksPath)
  }

  private def initDirectory(path: Path): Unit = {
    if (!fsUtils.exists(path)) {
      fsUtils.createDirectoryRecursive(path)
    }
  }

  private def initRecordsDirectory(path: Path): Unit = {
    if (!fsUtils.exists(path)) {
      fsUtils.createDirectoryRecursive(path)
      writeEmptyDataset[DataChunk](path.toUri.toString)
    }
  }

  private def initSchemasDirectory(path: Path): Unit = {
    if (!fsUtils.exists(path)) {
      fsUtils.createDirectoryRecursive(path)
      writeEmptyDataset[TableSchemaJson](path.toUri.toString)
    }
  }
}
