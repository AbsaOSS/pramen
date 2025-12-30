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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import za.co.absa.pramen.core.bookkeeper.model.TableSchemaJson
import za.co.absa.pramen.core.lock.TokenLockHadoopPath
import za.co.absa.pramen.core.model.{DataChunk, TableSchema}
import za.co.absa.pramen.core.utils.{CsvUtils, FsUtils, JsonUtils, SparkUtils}

import java.time.{Instant, LocalDate}
import scala.util.Random


object BookkeeperText {
  val bkFileName = "bookkeeping.csv"
  val schemasFileName = "schemas.json"
  val bookkeepingRootPath = "bk"
  val recordsDirName = "records_csv"
  val schemasDirName = "schemas_json"
  val locksDirName = "locks"
}

class BookkeeperText(bookkeepingPath: String, batchId: Long)(implicit spark: SparkSession) extends BookkeeperHadoop(batchId) {

  import BookkeeperText._
  import spark.implicits._

  private val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, bookkeepingPath)
  private val bkPath = new Path(bookkeepingPath, bookkeepingRootPath)
  private val recordsPath = new Path(bkPath, recordsDirName)
  private val bkFilePath = new Path(recordsPath, bkFileName)
  private val schemasPath = new Path(bkPath, schemasDirName)
  private val schemasFilePath = new Path(schemasPath, schemasFileName)
  private val locksPath = new Path(bookkeepingPath, locksDirName)

  private val csvRecordsSchema = SparkUtils.getStructType[DataChunk]
  private val jsonSchemasSchema = SparkUtils.getStructType[TableSchemaJson]

  init()

  override val bookkeepingEnabled: Boolean = true

  override def getLatestProcessedDateFromStorage(tableName: String, until: Option[LocalDate]): Option[LocalDate] = {
    val filter = until match {
      case Some(endDate) =>
        val endDateStr = getDateStr(endDate)
        col("tableName") === tableName && col("infoDate") <= endDateStr
      case None =>
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

  override def getDataChunksFromStorage(tableName: String, infoDate: LocalDate, batchId: Option[Long]): Seq[DataChunk] = {
    val infoDateFilter = getFilter(tableName, Option(infoDate), Option(infoDate), batchId)

    getData(infoDateFilter)
  }

  override def getLatestDataChunkFromStorage(table: String, infoDate: LocalDate): Option[DataChunk] = {
    getData(getFilter(table, Option(infoDate), Option(infoDate), None)).lastOption
  }

  def getDataChunksCountFromStorage(table: String, dateBegin: Option[LocalDate], dateEnd: Option[LocalDate]): Long = {
    getDf(getFilter(table, dateBegin, dateEnd, None)).count()
  }

  private[pramen] override def saveRecordCountToStorage(table: String,
                                                        infoDate: LocalDate,
                                                        inputRecordCount: Long,
                                                        outputRecordCount: Long,
                                                        jobStarted: Long,
                                                        jobFinished: Long): Unit = {
    val lock: TokenLockHadoopPath = getLock

    try {
      val dateStr = getDateStr(infoDate)

      val chunk = DataChunk(table, dateStr, dateStr, dateStr, inputRecordCount, outputRecordCount, jobStarted, jobFinished, batchId)
      val csv = CsvUtils.getRecord(chunk, '|')
      fsUtils.appendFile(bkFilePath, csv)

    } finally {
      lock.release()
    }
  }

  private def getLock: TokenLockHadoopPath = {
    val lock = new TokenLockHadoopPath("bookkeeping",
      spark.sparkContext.hadoopConfiguration,
      locksPath.toUri.toString)

    while (!lock.tryAcquire()) {
      val randomWait = Random.nextInt(1000) + 1000
      Thread.sleep(randomWait)
    }
    lock
  }

  private def initDirectoryWithFile(path: Path, fileName: String): Unit = {
    if (!fsUtils.exists(path)) {
      fsUtils.createDirectoryRecursive(path)
      fsUtils.writeFile(new Path(path, fileName), "")
    }
  }

  private def initLockDirectory(path: Path): Unit = {
    if (!fsUtils.exists(path)) {
      fsUtils.createDirectoryRecursive(path)
    }
  }

  private def init(): Unit = {
    initDirectoryWithFile(recordsPath, bkFileName)
    initDirectoryWithFile(schemasPath, schemasFileName)
    initLockDirectory(locksPath)
  }

  private def getDf(filter: Column): Dataset[DataChunk] = {
    val df = spark
      .read
      .option("sep", "|")
      .schema(csvRecordsSchema)
      .csv(recordsPath.toUri.toString)

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
      .sortBy(_.jobFinished)
  }

  override def getLatestSchema(table: String, until: LocalDate): Option[(StructType, LocalDate)] = {
    val filter = getFilter(table, None, Option(until), None)

    val df = spark
      .read
      .option("sep", "|")
      .schema(jsonSchemasSchema)
      .json(schemasPath.toUri.toString)

    val tableSchemaOpt = df.filter(filter)
      .orderBy(col("infoDate").desc, col("updatedTimestamp").desc)
      .as[TableSchema]
      .take(1)
      .headOption

    tableSchemaOpt.flatMap(tableSchema => {
      TableSchema.toSchemaAndDate(tableSchema)
    })
  }

  private[pramen] override def saveSchema(table: String, infoDate: LocalDate, schema: StructType): Unit = {
    val tableSchema = TableSchemaJson(table, infoDate.toString, schema.json, Instant.now().toEpochMilli)

    val lock: TokenLockHadoopPath = getLock

    try {
      val json = JsonUtils.asJson(tableSchema)
      fsUtils.appendFile(schemasFilePath, s"$json\n")
    } finally {
      lock.release()
    }
  }
}
