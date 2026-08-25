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

package za.co.absa.pramen.bulkload

import za.co.absa.pramen.bulkload.model.{BulkLoadDate, BulkLoadSize}

import java.time.LocalDate
import scala.collection.mutable.ListBuffer

object BulkLoadDateUtils {

  /**
    * Splits the given date range into a sequence of bulk load intervals according to the
    * requested bulk load size.
    *
    * Each returned element contains the information date of the output partition together with
    * the start and end dates of the data range that should be loaded for that partition. The
    * intervals are generated sequentially, starting at `dateFrom`, and each subsequent interval
    * begins on the day following the end of the previous one, until `dateTo` is reached.
    *
    * @param dateFrom the first date of the range to be split, inclusive.
    * @param dateTo   the last date of the range to be split, inclusive. It must not be earlier
    *                 than `dateFrom`, otherwise an `IllegalArgumentException` is thrown.
    * @param mode     the granularity of the generated intervals, e.g. monthly, quarterly or yearly.
    * @return the sequence of bulk load intervals covering the requested date range.
    */
  def getBulkLoadDates(dateFrom: LocalDate, dateTo: LocalDate, mode: BulkLoadSize): Seq[BulkLoadDate] = {
    if (dateFrom.isAfter(dateTo))
      throw new IllegalArgumentException("dateFrom must not be after dateTo")

    val bulkLoadDates = new ListBuffer[BulkLoadDate]
    var currentDate = dateFrom

    while (currentDate.isBefore(dateTo) || currentDate.isEqual(dateTo)) {
      mode match {
        case BulkLoadSize.Monthly =>
          val lastDayOfMonth = currentDate.withDayOfMonth(currentDate.lengthOfMonth())
          val dateTo0 = if (lastDayOfMonth.isAfter(dateTo)) dateTo else lastDayOfMonth
          bulkLoadDates += BulkLoadDate(currentDate, currentDate, dateTo0)
          currentDate = lastDayOfMonth.plusDays(1)
        case BulkLoadSize.Quarterly =>
          val lastMonthOfQuarter = ((currentDate.getMonthValue - 1) / 3) * 3 + 3
          val lastMonthDate = currentDate.withDayOfMonth(1).withMonth(lastMonthOfQuarter)
          val lastDayOfQuarter = lastMonthDate.withDayOfMonth(lastMonthDate.lengthOfMonth())
          val dateTo0 = if (lastDayOfQuarter.isAfter(dateTo)) dateTo else lastDayOfQuarter
          bulkLoadDates += BulkLoadDate(currentDate, currentDate, dateTo0)
          currentDate = lastDayOfQuarter.plusDays(1)
        case BulkLoadSize.Yearly =>
          val lastDayOfYear = currentDate.withDayOfYear(currentDate.lengthOfYear())
          val dateTo0 = if (lastDayOfYear.isAfter(dateTo)) dateTo else lastDayOfYear
          bulkLoadDates += BulkLoadDate(currentDate, currentDate, dateTo0)
          currentDate = lastDayOfYear.plusDays(1)
      }
    }

    bulkLoadDates.toSeq
  }

}
