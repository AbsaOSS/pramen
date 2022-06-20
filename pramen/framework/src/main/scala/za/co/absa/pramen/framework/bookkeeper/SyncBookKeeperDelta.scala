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

package za.co.absa.pramen.framework.bookkeeper

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Dataset, SaveMode, SparkSession}
import za.co.absa.pramen.framework.model.{DataChunk, TableSchema}
import za.co.absa.pramen.framework.utils.FsUtils

import java.time.LocalDate
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object SyncBookKeeperDelta {
  val bookkeepingRootPath = "bk"
  val recordsDirName = "records_delta"
  val schemasDirName = "schemas_delta"
  val locksDirName = "locks"
}

class SyncBookKeeperDelta(bookkeepingPath: String)(implicit spark: SparkSession) extends SyncBookKeeperHadoop {

  import SyncBookKeeperDelta._
  import spark.implicits._

  private val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, bookkeepingPath)
  private val bkPath = new Path(bookkeepingPath, bookkeepingRootPath)
  private val recordsPath = new Path(bkPath, recordsDirName)
  private val schemasPath = new Path(bkPath, schemasDirName)
  private val locksPath = new Path(bookkeepingPath, locksDirName)

  init()

  override val bookkeepingEnabled: Boolean = true

  override def getLatestProcessedDate(tableName: String, until: Option[LocalDate]): Option[LocalDate] = {
    val filter = until match {
      case Some(endDate) =>
        val endDateStr = getDateStr(endDate)
        col("tableName") === tableName && col("infoDate") <= endDateStr
      case None          =>
        col("tableName") === tableName
    }

    val chunks = getData(filter)

    if (chunks.isEmpty) {
      None
    } else {
      val chunk = chunks.maxBy(_.infoDateEnd)
      Option(LocalDate.parse(chunk.infoDateEnd, DataChunk.dateFormatter))
    }
  }

  override def getLatestDataChunk(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Option[DataChunk] = {
    getDataChunks(table, dateBegin, dateEnd).lastOption
  }

  override def getDataChunks(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Seq[DataChunk] = {
    val infoDateFilter = getFilter(tableName, Option(infoDateBegin), Option(infoDateEnd))

    getData(infoDateFilter)
  }

  def getDataChunksCount(table: String, dateBegin: Option[LocalDate], dateEnd: Option[LocalDate]): Long = {
    getDf(getFilter(table, dateBegin, dateEnd)).count()
  }

  private[pramen] override def setRecordCount(table: String,
                                               infoDate: LocalDate,
                                               infoDateBegin: LocalDate,
                                               infoDateEnd: LocalDate,
                                               inputRecordCount: Long,
                                               outputRecordCount: Long,
                                               jobStarted: Long,
                                               jobFinished: Long): Unit = {
    val dateStr = getDateStr(infoDate)
    val dateBeginStr = getDateStr(infoDateBegin)
    val dateEndStr = getDateStr(infoDateEnd)

    val chunk = DataChunk(table, dateStr, dateBeginStr, dateEndStr, inputRecordCount, outputRecordCount, jobStarted, jobFinished)

    val df = Seq(chunk).toDF()

    df.write
      .mode(SaveMode.Append)
      .format("delta")
      .save(recordsPath.toUri.toString)
  }

  override def getLatestSchema(table: String, until: LocalDate): Option[(StructType, LocalDate)] = {
    val filter = getFilter(table, None, Option(until))

    val df = spark
      .read
      .format("delta")
      .load(schemasPath.toUri.toString)

    val tableSchemaOpt = df.filter(filter)
      .orderBy(col("infoDate").desc)
      .as[TableSchema]
      .take(1)
      .headOption

    tableSchemaOpt.flatMap(tableSchema => {
      TableSchema.toSchemaAndDate(tableSchema)
    })
  }

  private[pramen] override def saveSchema(table: String, infoDate: LocalDate, schema: StructType): Unit = {
    val tableSchema = TableSchema(table, infoDate.toString, schema.json)

    val df = Seq(tableSchema).toDF()

    df.write
      .mode(SaveMode.Append)
      .format("delta")
      .save(schemasPath.toUri.toString)
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
      writeEmptyDataset[DataChunk](path)
    }
  }

  private def initSchemasDirectory(path: Path): Unit = {
    if (!fsUtils.exists(path)) {
      fsUtils.createDirectoryRecursive(path)
      writeEmptyDataset[TableSchema](path)
    }
  }

  private def writeEmptyDataset[T <: Product : TypeTag : ClassTag](path: Path): Unit = {
    val df = Seq.empty[T].toDS

    df.write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .save(path.toUri.toString)
  }

  private def getDf(filter: Column): Dataset[DataChunk] = {
    val df = spark
      .read
      .format("delta")
      .load(recordsPath.toUri.toString)

    df.filter(filter)
      .orderBy(col("jobFinished"))
      .as[DataChunk]
  }

  private def getData(filter: Column): Seq[DataChunk] = {
    getDf(filter)
      .collect()
      .groupBy(v => (v.tableName, v.infoDate))
      .map { case (_, listChunks) =>
        listChunks.maxBy(c => c.jobFinished)
      }
      .toArray[DataChunk]
  }
}
