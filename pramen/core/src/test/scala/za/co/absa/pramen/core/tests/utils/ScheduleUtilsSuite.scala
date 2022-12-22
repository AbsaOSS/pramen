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

package za.co.absa.pramen.core.tests.utils

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.schedule.Schedule
import za.co.absa.pramen.core.utils.ScheduleUtils

import java.time._

class ScheduleUtilsSuite extends AnyWordSpec {

  // Various schedules
  private val everyday = Schedule. EveryDay()
  private val onSundays = Schedule.Weekly(DayOfWeek.SUNDAY :: Nil)
  private val onSundaysAndWednesdays = Schedule.Weekly(DayOfWeek.WEDNESDAY :: DayOfWeek.SUNDAY :: Nil)
  private val onFirstDayOfMonth = Schedule.Monthly(1 :: Nil)

  // Sample dates to use
  private val saturday = LocalDate.of(2020, 8, 1)
  private val sunday = LocalDate.of(2020, 8, 2)
  private val monday = LocalDate.of(2020, 8, 3)
  private val tuesday = LocalDate.of(2020, 8, 4)
  private val wednesday = LocalDate.of(2020, 8, 5)
  private val thursday = LocalDate.of(2020, 8, 6)
  private val friday = LocalDate.of(2020, 8, 7)

  "fromInstanceToLocalDate()" should {
    "return a date from an instant" in {
      val ins1 = Instant.from(ZonedDateTime.of(friday, LocalTime.of(0, 0), ZoneId.systemDefault()))
      val ins2 = Instant.from(ZonedDateTime.of(friday, LocalTime.of(10, 0), ZoneId.systemDefault()))
      val ins3 = Instant.from(ZonedDateTime.of(friday, LocalTime.of(23, 59), ZoneId.systemDefault()))

      assert(ScheduleUtils.fromInstanceToLocalDate(ins1) == friday)
      assert(ScheduleUtils.fromInstanceToLocalDate(ins2) == friday)
      assert(ScheduleUtils.fromInstanceToLocalDate(ins3) == friday)
    }
  }

  "getDatesRange()" should {
    "return ranges of dates depending on schedule" when {
      "schedule is daily" in {
        assert(ScheduleUtils.getDatesRange(everyday, monday) == (monday, monday))
        assert(ScheduleUtils.getDatesRange(everyday, wednesday) == (wednesday, wednesday))
        assert(ScheduleUtils.getDatesRange(everyday, friday) == (friday, friday))
        assert(ScheduleUtils.getDatesRange(everyday, sunday) == (sunday, sunday))
        assert(ScheduleUtils.getDatesRange(everyday, saturday) == (saturday, saturday))
      }

      "schedule is weekly" in {
        assert(ScheduleUtils.getDatesRange(onSundays, monday) == (sunday.minusDays(6), sunday))
        assert(ScheduleUtils.getDatesRange(onSundays, wednesday) == (sunday.minusDays(6), sunday))
        assert(ScheduleUtils.getDatesRange(onSundays, friday) == (sunday.minusDays(6), sunday))
        assert(ScheduleUtils.getDatesRange(onSundays, sunday) == (sunday.minusDays(6), sunday))
        assert(ScheduleUtils.getDatesRange(onSundays, saturday) == (sunday.minusDays(13), sunday.minusDays(7)))
      }

      "schedule is by-weekly" in {
        assert(ScheduleUtils.getDatesRange(onSundaysAndWednesdays, monday) == (wednesday.minusDays(6), sunday))
        assert(ScheduleUtils.getDatesRange(onSundaysAndWednesdays, wednesday) == (monday, wednesday))
        assert(ScheduleUtils.getDatesRange(onSundaysAndWednesdays, friday) == (monday, wednesday))
        assert(ScheduleUtils.getDatesRange(onSundaysAndWednesdays, sunday) == (wednesday.minusDays(6), sunday))
        assert(ScheduleUtils.getDatesRange(onSundaysAndWednesdays, saturday) == (sunday.minusDays(6), wednesday.minusDays(7)))
      }

      "schedule is monthly" in {
        assert(ScheduleUtils.getDatesRange(onFirstDayOfMonth, monday) == (saturday.minusDays(30), saturday))
      }
    }
  }

  "getDatesRangeWithShift()" should {
    "return ranges of dates depending on schedule ans shift" when {
      "schedule is daily" in {
        assert(ScheduleUtils.getDatesRangeWithShift(everyday, monday, 0) == (monday, monday))
        assert(ScheduleUtils.getDatesRangeWithShift(everyday, wednesday, 1) == (thursday, thursday))
        assert(ScheduleUtils.getDatesRangeWithShift(everyday, friday, -1) == (thursday, thursday))
      }

      "schedule is weekly" in {
        assert(ScheduleUtils.getDatesRangeWithShift(onSundays, monday, 0) == (sunday.minusDays(6), sunday))
        assert(ScheduleUtils.getDatesRangeWithShift(onSundays, wednesday, 1) == (sunday.minusDays(5), sunday.plusDays(1)))
        assert(ScheduleUtils.getDatesRangeWithShift(onSundays, friday, -1) == (sunday.minusDays(7), saturday))
      }

      "schedule is by-weekly" in {
        assert(ScheduleUtils.getDatesRangeWithShift(onSundaysAndWednesdays, monday, 0) == (wednesday.minusDays(6), sunday))
        assert(ScheduleUtils.getDatesRangeWithShift(onSundaysAndWednesdays, wednesday, 1) == (tuesday, thursday))
        assert(ScheduleUtils.getDatesRangeWithShift(onSundaysAndWednesdays, friday, -1) == (sunday, tuesday))
      }

      "schedule is monthly" in {
        assert(ScheduleUtils.getDatesRangeWithShift(onFirstDayOfMonth, monday, 0) == (saturday.minusDays(30), saturday))
        assert(ScheduleUtils.getDatesRangeWithShift(onFirstDayOfMonth, monday, 1) == (saturday.minusDays(29), sunday))
        assert(ScheduleUtils.getDatesRangeWithShift(onFirstDayOfMonth, monday, -1) == (saturday.minusDays(31), saturday.minusDays(1)))
      }
    }
  }

  "getMissedEvent()" should {
    "return None if the last update date is recent" when {
      "schedule is daily" in {
        assert(ScheduleUtils.getMissedEvent(everyday, friday, friday).isEmpty)
        assert(ScheduleUtils.getMissedEvent(everyday, friday, wednesday).get == friday)
      }

      "schedule is weekly" in {
        assert(ScheduleUtils.getMissedEvent(onSundays, friday, monday).isEmpty)
        assert(ScheduleUtils.getMissedEvent(onSundays, friday, sunday).isEmpty)
        assert(ScheduleUtils.getMissedEvent(onSundays, friday, saturday).isDefined)
        assert(ScheduleUtils.getMissedEvent(onSundays, friday, saturday).get == sunday)
      }

      "schedule is by-weekly" in {
        assert(ScheduleUtils.getMissedEvent(onSundaysAndWednesdays, friday, friday).isEmpty)
        assert(ScheduleUtils.getMissedEvent(onSundaysAndWednesdays, friday, wednesday).isEmpty)
        assert(ScheduleUtils.getMissedEvent(onSundaysAndWednesdays, friday, monday).isDefined)
        assert(ScheduleUtils.getMissedEvent(onSundaysAndWednesdays, friday, monday).get == wednesday)
        assert(ScheduleUtils.getMissedEvent(onSundaysAndWednesdays, friday, sunday).isDefined)
        assert(ScheduleUtils.getMissedEvent(onSundaysAndWednesdays, friday, sunday).get == wednesday)
        assert(ScheduleUtils.getMissedEvent(onSundaysAndWednesdays, friday, saturday).isDefined)
        assert(ScheduleUtils.getMissedEvent(onSundaysAndWednesdays, friday, saturday).get == wednesday)
      }

      "schedule is monthly" in {
        assert(ScheduleUtils.getMissedEvent(onFirstDayOfMonth, friday, saturday).isEmpty)
        assert(ScheduleUtils.getMissedEvent(onFirstDayOfMonth, friday, saturday.minusDays(1)).get == saturday)
      }
    }

  }

  "getRecentActiveDay()" should {
    "return a correct date" when {
      "schedule is daily" in {
        assert(ScheduleUtils.getRecentActiveDay(everyday, friday) == friday)
      }

      "schedule is weekly" in {
        assert(ScheduleUtils.getRecentActiveDay(onSundays, friday) == sunday)
        assert(ScheduleUtils.getRecentActiveDay(onSundays, wednesday) == sunday)
        assert(ScheduleUtils.getRecentActiveDay(onSundays, monday) == sunday)
        assert(ScheduleUtils.getRecentActiveDay(onSundays, sunday) == sunday)
        assert(ScheduleUtils.getRecentActiveDay(onSundays, saturday) == sunday.minusDays(7))
      }

      "schedule is by-weekly" in {
        assert(ScheduleUtils.getRecentActiveDay(onSundaysAndWednesdays, friday) == wednesday)
        assert(ScheduleUtils.getRecentActiveDay(onSundaysAndWednesdays, wednesday) == wednesday)
        assert(ScheduleUtils.getRecentActiveDay(onSundaysAndWednesdays, monday) == sunday)
        assert(ScheduleUtils.getRecentActiveDay(onSundaysAndWednesdays, sunday) == sunday)
        assert(ScheduleUtils.getRecentActiveDay(onSundaysAndWednesdays, saturday) == wednesday.minusDays(7))
      }

      "schedule is monthly" in {
        assert(ScheduleUtils.getRecentActiveDay(onFirstDayOfMonth, friday) == saturday)
      }

    }
  }

  "getActiveDatesForPeriod()" should {
    "return a list of dates" when {
      "schedule is daily" in {
        val expectedOneDay = ScheduleUtils.getActiveDatesForPeriod(everyday, friday, friday)
        val expectedTwoDays = ScheduleUtils.getActiveDatesForPeriod(everyday, thursday, friday)
        val expectedSeveralDays = ScheduleUtils.getActiveDatesForPeriod(everyday, tuesday, friday)

        assert(expectedOneDay == Seq(friday))
        assert(expectedTwoDays == Seq(thursday, friday))
        assert(expectedSeveralDays == Seq(tuesday, wednesday, thursday, friday))
      }

      "schedule is weekly" in {
        val emptyExpected = ScheduleUtils.getActiveDatesForPeriod(onSundays, monday, friday)
        val sundayExpected = ScheduleUtils.getActiveDatesForPeriod(onSundays, sunday, friday)
        val twoSundaysExpected = ScheduleUtils.getActiveDatesForPeriod(onSundays, saturday, monday.plusDays(7))

        assert(emptyExpected.isEmpty)
        assert(sundayExpected == Seq(sunday))
        assert(twoSundaysExpected == Seq(sunday, sunday.plusDays(7)))
      }
    }
  }

  "getInformationDatesToCheck()" should {
    "return a list of dates" when {
      "schedule is daily" in {
        val expectedOneDay = ScheduleUtils.getInformationDatesToCheck(everyday, friday, friday, saturday, 1, 0, bookkeepingEnabled = true)
        val expectedTwoDays = ScheduleUtils.getInformationDatesToCheck(everyday, friday, wednesday, saturday, 1, 1, bookkeepingEnabled = true)
        val expectedSeveralDays = ScheduleUtils.getInformationDatesToCheck(everyday, friday, monday, saturday, 1, 2, bookkeepingEnabled = true)

        assert(expectedOneDay == Seq(thursday))
        assert(expectedTwoDays == Seq(wednesday, thursday))
        assert(expectedSeveralDays == Seq(tuesday, wednesday, thursday))
      }

      "schedule is weekly" in {
        val emptyExpected = ScheduleUtils.getInformationDatesToCheck(onSundays, friday, sunday, saturday, 1, 2, bookkeepingEnabled = true)
        val sundayExpected = ScheduleUtils.getInformationDatesToCheck(onSundays, friday, saturday, saturday, 1, 4, bookkeepingEnabled = true)
        val twoSundaysExpected = ScheduleUtils.getInformationDatesToCheck(onSundays, monday.plusDays(7), saturday, saturday, 1, 4, bookkeepingEnabled = true)

        assert(emptyExpected.isEmpty)
        assert(sundayExpected == Seq(sunday))
        assert(twoSundaysExpected == Seq(sunday, sunday.plusDays(7)))
      }

      "schedule is by-weekly" in {
        val emptyExpected = ScheduleUtils.getInformationDatesToCheck(onSundaysAndWednesdays, friday, wednesday, saturday, 0, 1, bookkeepingEnabled = true)
        val wednesdayExpected = ScheduleUtils.getInformationDatesToCheck(onSundaysAndWednesdays, friday, wednesday, saturday, 1, 1, bookkeepingEnabled = true)
        val sundayAndWednesdayExpected = ScheduleUtils.getInformationDatesToCheck(onSundaysAndWednesdays, friday, saturday, saturday, 1, 4, bookkeepingEnabled = true)
        val severalDaysExpected = ScheduleUtils.getInformationDatesToCheck(onSundaysAndWednesdays, friday.plusDays(7), saturday, saturday, 1, 4, bookkeepingEnabled = true)

        assert(emptyExpected.isEmpty)
        assert(wednesdayExpected == Seq(wednesday))
        assert(sundayAndWednesdayExpected == Seq(sunday, wednesday))
        assert(severalDaysExpected == Seq(sunday, wednesday, sunday.plusDays(7), wednesday.plusDays(7)))
      }

      "schedule is monthly" in {
        val emptyExpected = ScheduleUtils.getInformationDatesToCheck(onFirstDayOfMonth, friday, saturday, saturday, 1, 0, bookkeepingEnabled = true)
        val firstOfAugustExpected = ScheduleUtils.getInformationDatesToCheck(onFirstDayOfMonth, friday, saturday.minusDays(2), saturday.minusDays(31), 1, 0, bookkeepingEnabled = true)

        assert(emptyExpected == Nil)
        assert(firstOfAugustExpected == Seq(saturday))
      }

      "schedule is monthly and the minimum date is very recent" in {
        val emptyExpected1 = ScheduleUtils.getInformationDatesToCheck(onFirstDayOfMonth, thursday, friday, saturday, 1, 0, bookkeepingEnabled = true)
        val emptyExpected2 = ScheduleUtils.getInformationDatesToCheck(onFirstDayOfMonth, thursday, saturday.minusDays(2), sunday, 1, 0, bookkeepingEnabled = true)

        assert(emptyExpected1 == Nil)
        assert(emptyExpected2 == Nil)
      }

      "a regression is fixed" in {
        val day0 = LocalDate.of(2020, 8, 12)
        val curDay = LocalDate.of(2020, 8, 16)

        val dates = ScheduleUtils.getInformationDatesToCheck(onSundays, curDay, day0, day0, 1, 2, bookkeepingEnabled = true)

        assert(dates.nonEmpty)
      }
    }
  }

  "getLateInformationDatesToCheck()" should {
    "return a list of dates" when {
      "schedule is daily" in {
        val emptyExpected1 = ScheduleUtils.getLateInformationDatesToCheck(everyday, friday, thursday, saturday, 1)
        val emptyExpected2 = ScheduleUtils.getLateInformationDatesToCheck(everyday, friday, wednesday, saturday, 1)
        val expectedOneDay = ScheduleUtils.getLateInformationDatesToCheck(everyday, friday, tuesday, saturday, 1)
        val expectedTwoDays = ScheduleUtils.getLateInformationDatesToCheck(everyday, friday, monday, saturday, 1)

        assert(emptyExpected1.isEmpty)
        assert(emptyExpected2.isEmpty)
        assert(expectedOneDay == Seq(wednesday))
        assert(expectedTwoDays == Seq(tuesday, wednesday))
      }

      "schedule is weekly" in {
        val emptyExpected = ScheduleUtils.getLateInformationDatesToCheck(onSundays, friday, sunday, saturday, 1)
        val oneSundayExpected = ScheduleUtils.getLateInformationDatesToCheck(onSundays, friday, saturday, saturday, 1)
        val twoSundaysExpected = ScheduleUtils.getLateInformationDatesToCheck(onSundays, monday.plusDays(7), saturday, saturday, 0)

        assert(emptyExpected.isEmpty)
        assert(oneSundayExpected == Seq(sunday))
        assert(twoSundaysExpected == Seq(sunday, sunday.plusDays(7)))
      }

      "schedule is by-weekly" in {
        val emptyExpected = ScheduleUtils.getLateInformationDatesToCheck(onSundaysAndWednesdays, friday, wednesday, saturday, 0)
        val wednesdayExpected = ScheduleUtils.getLateInformationDatesToCheck(onSundaysAndWednesdays, friday, wednesday, saturday, 1)
        val sundayAndWednesdayExpected = ScheduleUtils.getLateInformationDatesToCheck(onSundaysAndWednesdays, friday, saturday, saturday, 1)
        val severalDaysExpected = ScheduleUtils.getLateInformationDatesToCheck(onSundaysAndWednesdays, friday.plusDays(7), saturday, saturday, 1)

        assert(emptyExpected.isEmpty)
        assert(wednesdayExpected.isEmpty)
        assert(sundayAndWednesdayExpected == Seq(sunday, wednesday))
        assert(severalDaysExpected == Seq(sunday, wednesday, sunday.plusDays(7), wednesday.plusDays(7)))
      }

      "schedule is monthly" in {
        val emptyExpected = ScheduleUtils.getLateInformationDatesToCheck(onFirstDayOfMonth, friday, saturday, saturday, 1)
        val firstOfAugustExpected = ScheduleUtils.getLateInformationDatesToCheck(onFirstDayOfMonth, friday, saturday.minusDays(2), saturday.minusDays(31), 1)

        assert(emptyExpected == Nil)
        assert(firstOfAugustExpected == Seq(saturday))
      }

      "should not return current expected date" in {
        val day0 = LocalDate.of(2020, 8, 12)
        val curDay = LocalDate.of(2020, 8, 16)

        val dates = ScheduleUtils.getLateInformationDatesToCheck(onSundays, curDay, day0, day0, 1)

        assert(dates.isEmpty)
      }

      "should return a late date" in {
        val day0 = LocalDate.of(2020, 8, 12)
        val curDay = LocalDate.of(2020, 8, 20)

        val dates = ScheduleUtils.getLateInformationDatesToCheck(onSundays, curDay, day0, day0, 1)

        assert(dates == Seq(LocalDate.of(2020, 8, 16)))
      }
    }
  }

}
