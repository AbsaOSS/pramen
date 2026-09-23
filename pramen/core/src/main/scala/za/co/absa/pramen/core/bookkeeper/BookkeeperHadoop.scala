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

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, Dataset}
import za.co.absa.pramen.core.bookkeeper.model.{DataAvailability, DataAvailabilityAggregation}
import za.co.absa.pramen.core.model.DataChunk

import java.time.LocalDate


abstract class BookkeeperHadoop(batchId: Long) extends BookkeeperBase(true, batchId) {
  private[core] def getFilter(tableName: String, infoDateBegin: Option[LocalDate], infoDateEnd: Option[LocalDate], batchId: Option[Long]): Column = {
    val baseFilter = (infoDateBegin, infoDateEnd) match {
      case (Some(begin), Some(end)) =>
        val beginStr = getDateStr(begin)
        val endStr = getDateStr(end)
        col("tableName") === tableName && col("infoDate") >= beginStr && col("infoDate") <= endStr
      case (Some(begin), None) =>
        val beginStr = getDateStr(begin)
        col("tableName") === tableName && col("infoDate") >= beginStr
      case (None, Some(end)) =>
        val endStr = getDateStr(end)
        col("tableName") === tableName && col("infoDate") <= endStr
      case (None, None) =>
        col("tableName") === tableName
    }

    batchId match {
      case Some(id) => baseFilter && col("batchId") === lit(id)
      case None => baseFilter
    }
  }

  private[core] def getDataAvailabilityFromDf(filteredChunkDf: Dataset[DataChunk]): Seq[DataAvailability] = {
    implicit val encoder: ExpressionEncoder[DataAvailabilityAggregation] = ExpressionEncoder[DataAvailabilityAggregation]

    val grouped = filteredChunkDf.groupBy("infoDate")
      .agg(
        count(lit(1)).cast(IntegerType).as("chunks"),
        sum("outputRecordCount").as("totalRecords")
      )
      .orderBy(col("infoDate").asc)
      .as[DataAvailabilityAggregation]

    val tuples = grouped.collect()

    tuples.map(t => DataAvailability(LocalDate.parse(t.infoDate, DataChunk.dateFormatter), t.chunks, t.totalRecords))
  }
}
