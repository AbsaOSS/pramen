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

package za.co.absa.pramen.core.utils

import java.time._
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import scala.annotation.tailrec

object DateUtils {
  // Include this line if you need to sort containers by a date:
  // import za.co.absa.pramen.core.utils.DateUtils._
  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)

  val isoDatePattern = "yyyy-MM-dd"

  val isoDateTimePattern = "yyyy-MM-dd HH:mm:ss"

  val isoDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(isoDatePattern)

  val isoDateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(isoDateTimePattern)

  /**
    * Converts a date from string to date by trying to apply a more than 1 format.
    *
    * @param dateStr A date represented as string
    * @param patterns Possible date patterns
    * @return The date. Throws `IllegalArgumentException` if it doesn't work for any of the input patterns.
    */
  def convertStrToDate(dateStr: String, patterns: String*): LocalDate = {
    if (patterns.isEmpty) {
      throw new IllegalArgumentException(s"No patterns provided to parse date '$dateStr'")
    }

    @tailrec
    def convertStrToDateHelper(patternsInner: String*): LocalDate = {
      if (patternsInner.isEmpty) {
        throw new IllegalArgumentException(s"Cannot parse '$dateStr' in one of ${patterns.map(a => s"'$a'").mkString(", ")}")
      }

      try {
        val fmt = DateTimeFormatter.ofPattern(patternsInner.head)
        LocalDate.parse(dateStr, fmt)
      } catch {
        case _: DateTimeParseException =>
          convertStrToDateHelper(patternsInner.tail: _*)
      }
    }

    convertStrToDateHelper(patterns: _*)
  }

  def fromIsoStrToDate(dateStr: String): LocalDate = {
    LocalDate.parse(dateStr, isoDateFormatter)
  }

  def fromIsoStrToTimestampMs(dateStr: String, timezoneId: ZoneId): Long = {
    fromDateToTimestampMs(LocalDate.parse(dateStr, isoDateFormatter), timezoneId)
  }

  def fromDateToIsoStr(date: LocalDate): String = {
    date.format(isoDateFormatter)
  }

  def fromDateAndTimeToIsoStr(date: LocalDate, time: LocalTime): String = {
    val dateTime = LocalDateTime.of(date, time)
    dateTime.format(isoDateTimeFormatter)
  }

  def fromDateToTimestampMs(date: LocalDate, timezoneId: ZoneId): Long = {
    val time = LocalTime.of(0, 0, 0)
    val zonedDateTime = ZonedDateTime.of(date, time, timezoneId)
    Instant.from(zonedDateTime).toEpochMilli
  }


}
