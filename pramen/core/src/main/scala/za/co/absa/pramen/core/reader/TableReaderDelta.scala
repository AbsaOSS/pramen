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

package za.co.absa.pramen.core.reader

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{Query, TableReader}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class TableReaderDelta(infoDateColumn: String,
                       infoDateFormat: String = "yyyy-MM-dd"
                      )(implicit spark: SparkSession) extends TableReader {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern(infoDateFormat)

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    if (infoDateBegin.equals(infoDateEnd)) {
      log.info(s"Reading COUNT(*) FROM ${query.query} WHERE $infoDateColumn='${dateFormatter.format(infoDateBegin)}'")
    } else {
      log.info(s"Reading COUNT(*) FROM ${query.query} WHERE $infoDateColumn BETWEEN '${dateFormatter.format(infoDateBegin)}' AND '${dateFormatter.format(infoDateEnd)}'")
    }
    getFilteredDataFrame(query, infoDateBegin, infoDateEnd).count()
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    if (infoDateBegin.equals(infoDateEnd)) {
      log.info(s"Reading * FROM ${query.query} WHERE $infoDateColumn='${dateFormatter.format(infoDateEnd)}'")
    } else {
      log.info(s"Reading * FROM ${query.query} WHERE $infoDateColumn BETWEEN '${dateFormatter.format(infoDateBegin)}' AND '${dateFormatter.format(infoDateEnd)}'")
    }
    getFilteredDataFrame(query, infoDateBegin, infoDateEnd)
  }

  private def getFilteredDataFrame(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): DataFrame = {
    val infoDateBeginStr = dateFormatter.format(infoDateBegin)
    val infoDateEndStr = dateFormatter.format(infoDateEnd)

    val reader = query match {
      case Query.Path(path)   =>
        spark
          .read
          .format("delta")
          .load(path)
      case Query.Table(table) =>
        spark
          .table(table)
      case _                  => throw new IllegalArgumentException(s"Arguments of type ${query.getClass} are not supported for Delta format.")
    }

    if (infoDateBegin.equals(infoDateEnd)) {
      reader.filter(col(s"$infoDateColumn") === lit(infoDateBeginStr))
    } else {
      reader.filter(col(s"$infoDateColumn") >= lit(infoDateBeginStr) && col(s"$infoDateColumn") <= lit(infoDateEndStr))
    }
  }
}
