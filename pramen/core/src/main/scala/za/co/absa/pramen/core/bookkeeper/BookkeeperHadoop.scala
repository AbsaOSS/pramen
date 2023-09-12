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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import za.co.absa.pramen.core.model.DataChunk

import java.time.LocalDate


abstract class BookkeeperHadoop extends BookkeeperBase(true) {
  private[core] def getFilter(tableName: String, infoDateBegin: Option[LocalDate], infoDateEnd: Option[LocalDate]): Column = {
    (infoDateBegin, infoDateEnd) match {
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
  }
}
