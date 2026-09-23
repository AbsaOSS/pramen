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

package za.co.absa.pramen.bulkload.model

import java.time.LocalDate
import java.time.format.DateTimeParseException

import scala.util.control.NonFatal

case class BulkLoadState(pramenTableName: String,
                         infoDateColumn: String,
                         outputInfoDate: LocalDate,
                         dataDateFrom: LocalDate,
                         dataDateTo: LocalDate,
                         phase: BulkLoadPhase)

object BulkLoadState {
  def toSerialized(bulkLoadState: BulkLoadState): BulkLoadStateSerialized = {
    BulkLoadStateSerialized(
      bulkLoadState.pramenTableName,
      bulkLoadState.infoDateColumn,
      bulkLoadState.outputInfoDate.toString,
      bulkLoadState.dataDateFrom.toString,
      bulkLoadState.dataDateTo.toString,
      bulkLoadState.phase.name
    )
  }

  def fromSerialized(bulkLoadStateSerialized: BulkLoadStateSerialized): BulkLoadState = {
    BulkLoadState(
      bulkLoadStateSerialized.pramenTableName,
      bulkLoadStateSerialized.infoDateColumn,
      parseDate(bulkLoadStateSerialized.outputInfoDate, "outputInfoDate"),
      parseDate(bulkLoadStateSerialized.dataDateFrom, "dataDateFrom"),
      parseDate(bulkLoadStateSerialized.dataDateTo, "dataDateTo"),
      BulkLoadPhase.fromString(bulkLoadStateSerialized.phase)
    )
  }

  private def parseDate(dateStr: String, fieldName: String): LocalDate = {
    try {
      LocalDate.parse(dateStr)
    } catch {
      case NonFatal(ex: DateTimeParseException) =>
        throw new IllegalArgumentException(s"Cannot parse the bulk load state field '$fieldName'. Expected an ISO date (yyyy-MM-dd), got '$dateStr'.", ex)
    }
  }

}
