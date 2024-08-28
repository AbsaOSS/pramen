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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Dataset}
import za.co.absa.pramen.core.bookkeeper.model.TableSchemaJson
import za.co.absa.pramen.core.model.{DataChunk, TableSchema}

import java.time.LocalDate
import scala.reflect.ClassTag
import scala.reflect.runtime.universe

abstract class BookkeeperDeltaBase extends BookkeeperHadoop {

  def getBkDf(filter: Column): Dataset[DataChunk]

  def saveRecordCountDelta(dataChunk: DataChunk): Unit

  def getSchemasDeltaDf: Dataset[TableSchemaJson]

  def saveSchemaDelta(schemas: TableSchema): Unit

  def writeEmptyDataset[T <: Product : universe.TypeTag : ClassTag](pathOrTable: String): Unit

  final override val bookkeepingEnabled: Boolean = true

  final override def getLatestProcessedDateFromStorage(tableName: String, until: Option[LocalDate]): Option[LocalDate] = {
    val filter = until match {
      case Some(endDate) =>
        val endDateStr = getDateStr(endDate)
        col("tableName") === tableName && col("infoDate") <= endDateStr
      case None =>
        col("tableName") === tableName
    }

    val chunks = getBkData(filter)

    if (chunks.isEmpty) {
      None
    } else {
      val chunk = chunks.maxBy(_.infoDateEnd)
      Option(LocalDate.parse(chunk.infoDateEnd, DataChunk.dateFormatter))
    }
  }

  final override def getLatestDataChunkFromStorage(table: String, dateBegin: LocalDate, dateEnd: LocalDate): Option[DataChunk] = {
    getDataChunks(table, dateBegin, dateEnd).lastOption
  }

  final override def getDataChunksFromStorage(tableName: String, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Seq[DataChunk] = {
    val infoDateFilter = getFilter(tableName, Option(infoDateBegin), Option(infoDateEnd))

    getBkData(infoDateFilter)
  }

  final def getDataChunksCountFromStorage(table: String, dateBegin: Option[LocalDate], dateEnd: Option[LocalDate]): Long = {
    getBkDf(getFilter(table, dateBegin, dateEnd)).count()
  }

  final private[pramen] override def saveRecordCountToStorage(table: String,
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

    saveRecordCountDelta(chunk)
  }

  final override def getLatestSchema(table: String, until: LocalDate): Option[(StructType, LocalDate)] = {
    val filter = getFilter(table, None, Option(until))

    val df = getSchemasDeltaDf

    val tableSchemaOpt = df.filter(filter)
      .orderBy(col("infoDate").desc, col("updatedTimestamp").desc)
      .take(1)
      .headOption

    tableSchemaOpt.flatMap(s => {
      TableSchema.toSchemaAndDate(TableSchema(s.tableName, s.infoDate, s.schemaJson))
    })
  }

  private[pramen] override def saveSchema(table: String, infoDate: LocalDate, schema: StructType): Unit = {
    val tableSchema = TableSchema(table, infoDate.toString, schema.json)

    saveSchemaDelta(tableSchema)
  }

  private[core] def getBkData(filter: Column): Seq[DataChunk] = {
    getBkDf(filter)
      .collect()
      .groupBy(v => (v.tableName, v.infoDate))
      .map { case (_, listChunks) =>
        listChunks.maxBy(c => c.jobFinished)
      }
      .toArray[DataChunk]
      .sortBy(_.infoDate)
  }
}
