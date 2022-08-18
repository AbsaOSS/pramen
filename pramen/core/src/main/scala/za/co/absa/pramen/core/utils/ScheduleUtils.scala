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

import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.schedule.Schedule

import java.time.{Instant, LocalDate, ZoneId, ZonedDateTime}
import scala.collection.mutable.ListBuffer

object ScheduleUtils {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Converts a moment in time into a date by applying local time zone.
    *
    * @param instant An instant
    * @return A date of the instant in the local time zone.
    */
  def fromInstanceToLocalDate(instant: Instant): LocalDate = {
    ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()).toLocalDate
  }

  /**
    * Converts an epoch second timesamp into a date-time by applying local time zone.
    *
    * @param epochSecond An epoch second timestamp
    * @return A date-time of the instant in the local time zone.
    */
  def fromEpochSecondToLocalDate(epochSecond: Long): ZonedDateTime = {
    val instant = Instant.ofEpochSecond(epochSecond)
    ZonedDateTime.ofInstant(instant, ZoneId.systemDefault())
  }

  /**
    * Returns dates range for a given schedule and information date
    *
    * @param schedule A schedule
    * @param infoDate An info date
    * @return Dates range
    */
  def getDatesRange(schedule: Schedule, infoDate: LocalDate): (LocalDate, LocalDate) = {
    val date1 = getRecentActiveDay(schedule, infoDate)
    val date0 = getRecentActiveDay(schedule, date1.minusDays(1)).plusDays(1)
    (date0, date1)
  }

  /**
    * Returns dates range with a specified shift (days)
    *
    * @param schedule  A schedule
    * @param infoDate  An info date
    * @param shiftDays The number of days by which to shift the period.
    * @return Dates range
    */
  def getDatesRangeWithShift(schedule: Schedule, infoDate: LocalDate, shiftDays: Int): (LocalDate, LocalDate) = {
    val (date0, date1) = getDatesRange(schedule, infoDate)
    (date0.plusDays(shiftDays), date1.plusDays(shiftDays))
  }

  /**
    * Returns is a scheduler missed an event. Maybe the job wasn't ready at the expected day.
    *
    * The method won't catch multiple missed events. In this case it will return the date of the
    * most recent event missed.
    *
    * @param schedule          A schedule.
    * @param date              A date to check against.
    * @param outputLastUpdated The date when last event has happened.
    * @return A date of missed event, if any.
    */
  def getMissedEvent(schedule: Schedule, date: LocalDate, outputLastUpdated: LocalDate): Option[LocalDate] = {
    val latestEvent = getRecentActiveDay(schedule, date)
    if (outputLastUpdated.isBefore(latestEvent)) {
      Some(latestEvent)
    } else {
      None
    }
  }

  /**
    * Gets the most recent date when en event was scheduled to run.
    *
    * @param schedule A schedule.
    * @param date     A starring date.
    * @return A date for which an event was scheduled. Should be the starting date or earlier.
    */
  def getRecentActiveDay(schedule: Schedule, date: LocalDate): LocalDate = {
    var outputDate = date
    while (!schedule.isEnabled(outputDate)) {
      outputDate = outputDate.minusDays(1)
    }
    outputDate
  }

  /**
    * Gets the next available information date event is scheduled to run.
    *
    * @param schedule A schedule.
    * @param date     A starring date.
    * @return A next active date if the passed date is not active.
    */
  def getNextActiveDay(schedule: Schedule, date: LocalDate): LocalDate = {
    var outputDate = date
    while (!schedule.isEnabled(outputDate)) {
      outputDate = outputDate.plusDays(1)
    }
    outputDate
  }

  /**
    * Gets a list of active dates to check for a given period and schedule.
    *
    * @param schedule  A schedule.
    * @param dateBegin A start date.
    * @param dateEnd   An end date.
    * @return A list of active dates.
    */
  def getActiveDatesForPeriod(schedule: Schedule, dateBegin: LocalDate, dateEnd: LocalDate): Seq[LocalDate] = {
    val infoDates = new ListBuffer[LocalDate]
    var day = dateBegin
    while (day.compareTo(dateEnd) <= 0) {
      if (schedule.isEnabled(day)) {
        infoDates += day
      }
      day = day.plusDays(1)
    }
    infoDates.toSeq
  }

  /**
    * Gets a list of information dates to check for a given schedule.
    *
    * @param schedule           A schedule.
    * @param currentDate        A current date.
    * @param latestEventDate    Latest time an event happened.
    * @param minimumDate        Minimum date beyond which there is no need to check.
    * @param expectedDelay      Expected delay in event happening.
    * @param trackDays          A number of days to look back for late event.
    * @param bookkeepingEnabled Specifies if bookkeeping is enabled and the job might need to check range of dates
    * @return
    */
  def getInformationDatesToCheck(schedule: Schedule,
                                 currentDate: LocalDate,
                                 latestEventDate: LocalDate,
                                 minimumDate: LocalDate,
                                 expectedDelay: Int,
                                 trackDays: Int,
                                 bookkeepingEnabled: Boolean
                                ): Seq[LocalDate] = {

    val dateWithDelay = currentDate.minusDays(expectedDelay)
    val nextActiveDay = getNextActiveDay(schedule, dateWithDelay)

    // The last day to check should not be later that the current date or the date with delay (if bookkeeping is disabled)
    val dateEnd = if (!bookkeepingEnabled || nextActiveDay.isAfter(currentDate)) {
      dateWithDelay
    } else {
      nextActiveDay
    }

    // Start checking the number of days before the expected event
    val trackDate = dateEnd
      .minusDays(trackDays)

    // If the last event happened earlier, start check from that date
    val nextDayAfterLastEvent = latestEventDate.plusDays(1)
    val trackDate2 = if (nextDayAfterLastEvent.isBefore(trackDate)) {
      nextDayAfterLastEvent
    } else {
      trackDate
    }

    // Never check past minimumDate
    val beginDate = if (trackDate2.isBefore(minimumDate)) {
      minimumDate
    } else {
      trackDate2
    }

    val infoDates = getActiveDatesForPeriod(schedule, beginDate, dateEnd)

    log.debug(s"Schedule: current date = $currentDate, last event date = $latestEventDate, " +
      s"minimumDate = $minimumDate, expectedDelay = $expectedDelay, trackDays = $trackDays => $infoDates")
    infoDates
  }

  /**
    * Gets a list of information dates for which data is ate for a given schedule.
    *
    * @param schedule        A schedule.
    * @param currentDate     A current date.
    * @param latestEventDate Latest time an event happened.
    * @param minimumDate     Minimum date beyond which there is no need to check.
    * @param expectedDelay   Expected delay in event happening.
    * @return
    */
  def getLateInformationDatesToCheck(schedule: Schedule,
                                     currentDate: LocalDate,
                                     latestEventDate: LocalDate,
                                     minimumDate: LocalDate,
                                     expectedDelay: Int): Seq[LocalDate] = {
    val dateWithDelay = currentDate.minusDays(expectedDelay + 1)
    val prevActiveDay = getRecentActiveDay(schedule, dateWithDelay)

    // If the last event happened earlier, start check from that date
    val nextDayAfterLastEvent = latestEventDate.plusDays(1)

    // Never check past minimumDate
    val beginDate = if (nextDayAfterLastEvent.isBefore(minimumDate)) {
      minimumDate
    } else {
      nextDayAfterLastEvent
    }

    val infoDates = getActiveDatesForPeriod(schedule, beginDate, prevActiveDay)

    log.info(s"Checking late dates: Schedule: current date = $currentDate, last event date = $latestEventDate, " +
      s"minimumDate = $minimumDate, expectedDelay = $expectedDelay => $infoDates")
    infoDates
  }

}
