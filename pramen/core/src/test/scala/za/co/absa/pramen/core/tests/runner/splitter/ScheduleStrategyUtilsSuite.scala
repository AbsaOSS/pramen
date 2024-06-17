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

package za.co.absa.pramen.core.tests.runner.splitter

import org.mockito.Mockito.{mock, when}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.status.{MetastoreDependency, TaskRunReason}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.expr.exceptions.SyntaxErrorException
import za.co.absa.pramen.core.model.DataChunk
import za.co.absa.pramen.core.pipeline
import za.co.absa.pramen.core.runner.splitter.RunMode
import za.co.absa.pramen.core.runner.splitter.ScheduleStrategyUtils._
import za.co.absa.pramen.core.schedule.Schedule

import java.time.{DayOfWeek, LocalDate}

class ScheduleStrategyUtilsSuite extends AnyWordSpec {
  "ScheduleStrategyCommon" when {
    val date = LocalDate.of(2022, 2, 18)

    "getRerun" should {
      val bk = mock(classOf[Bookkeeper])
      when(bk.getLatestDataChunk("table", date.minusDays(1), date.minusDays(1))).thenReturn(Some(null))
      when(bk.getLatestDataChunk("table", date, date)).thenReturn(None)

      "return information date of the rerun" in {
        val expected = pipeline.TaskPreDef(date.minusDays(1), TaskRunReason.Rerun)

        assert(getRerun("table", date, Schedule.EveryDay(), "@runDate - 1", bk) == expected :: Nil)
      }

      "return information date of the rerun with New status if the job hasn't ran yet" in {
        val expected = pipeline.TaskPreDef(date, TaskRunReason.New)

        assert(getRerun("table", date, Schedule.EveryDay(), "@runDate", bk) == expected :: Nil)
      }

      "return information date of a non-daily run rerun" in {
        val expected = pipeline.TaskPreDef(date.minusDays(1), TaskRunReason.Rerun)

        assert(getRerun("table", date, Schedule.Monthly(18 :: Nil), "@runDate - 1", bk) == expected :: Nil)
      }

      "return nothing is out of schedule rerun" in {
        assert(getRerun("table", date, Schedule.Monthly(1 :: Nil), "@runDate - 1", bk).isEmpty)
      }
    }

    "getNew" should {
      "return the date if it is scheduled to run on this date" in {
        assert(getNew("table", date, Schedule.EveryDay(), "@runDate").exists(t => t.infoDate == date))
      }

      "return None is out of schedule" in {
        assert(getNew("table", date, Schedule.Weekly(DayOfWeek.MONDAY :: Nil), "@runDate").isEmpty)
      }

      "returns the information date from the run date" in {
        assert(getNew("table", date, Schedule.Weekly(DayOfWeek.FRIDAY :: Nil), "@runDate - 1").exists(t => t.infoDate == date.minusDays(1)))
      }
    }

    "getLate" should {
      "return the date range when job hasn't ran yet" in {
        val expected = List(date.minusDays(3), date.minusDays(2), date.minusDays(1))
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))

        val actual = getLate("table", date, Schedule.EveryDay(), "@runDate", "@runDate - 3", None)

        assert(actual == expected)
      }

      "return the date range when there is some late data" in {
        val expected = List(date.minusDays(1))
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))

        val actual = getLate("table", date, Schedule.EveryDay(), "@runDate", "@runDate - 3", Some(date.minusDays(2)))

        assert(actual == expected)
      }

      "return empty list when data is up to date" in {
        val actual = getLate("table", date, Schedule.EveryDay(), "@runDate", "@runDate - 3", Some(date.minusDays(1)))

        assert(actual.isEmpty)
      }
    }

    "getHistorical" should {
      "return date range with completed jobs skipped" in {
        val bk = mock(classOf[Bookkeeper])

        val schedule = Schedule.Weekly(DayOfWeek.MONDAY :: DayOfWeek.WEDNESDAY :: DayOfWeek.FRIDAY :: Nil)

        when(bk.getDataChunksCount("table", Some(date), Some(date))).thenReturn(1)
        when(bk.getDataChunksCount("table", Some(date.plusDays(3)), Some(date.plusDays(3)))).thenReturn(1)
        when(bk.getDataChunksCount("table", Some(date.plusDays(5)), Some(date.plusDays(5)))).thenReturn(1)
        when(bk.getDataChunksCount("table", Some(date.plusDays(12)), Some(date.plusDays(12)))).thenReturn(1)

        val expected = List(date.plusDays(7), date.plusDays(10), date.plusDays(14))
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

        val actual = getHistorical("table", date, date.plusDays(14), schedule, RunMode.SkipAlreadyRan, "@runDate", date.minusDays(3), inverseDateOrder = false, bk)

        assert(actual == expected)
      }

      "return date range without completed jobs skipped" in {
        val bk = mock(classOf[Bookkeeper])

        val schedule = Schedule.Weekly(DayOfWeek.MONDAY :: DayOfWeek.WEDNESDAY :: DayOfWeek.FRIDAY :: Nil)

        val expected = List(date, date.plusDays(3), date.plusDays(5))
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

        val actual = getHistorical("table", date, date.plusDays(5), schedule, RunMode.CheckUpdates, "@runDate", date.minusDays(3), inverseDateOrder = false, bk)

        assert(actual == expected)
      }

      "return date range with reverse order" in {
        val bk = mock(classOf[Bookkeeper])

        val schedule = Schedule.Weekly(DayOfWeek.MONDAY :: DayOfWeek.WEDNESDAY :: DayOfWeek.FRIDAY :: Nil)

        val expected = List(date.plusDays(5), date.plusDays(3), date)
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

        val actual = getHistorical("table", date, date.plusDays(5), schedule, RunMode.CheckUpdates, "@runDate", date.minusDays(3), inverseDateOrder = true, bk)

        assert(actual == expected)
      }
    }

    "anyDependencyUpdatedRetrospectively" should {
      "return false if no dependencies are specified" in {
        assert(!anyDependencyUpdatedRetrospectively("table2", date, Nil, null))
      }

      "return false if a dependency is specified that has no retrospective updates" in {
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false, isPassive = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table2", date, date))
          .thenReturn(Some(DataChunk("table2", date.toString, date.toString, date.toString, 100, 100, 20000, 21000)))

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 10000, 11000)))

        assert(!anyDependencyUpdatedRetrospectively("table2", date, dep :: Nil, bk))
      }

      "return true if a dependency is specified that has retrospective updates" in {
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false, isPassive = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table2", date, date))
          .thenReturn(Some(DataChunk("table2", date.toString, date.toString, date.toString, 100, 100, 20000, 21000)))

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 30000, 31000)))

        assert(anyDependencyUpdatedRetrospectively("table2", date, dep :: Nil, bk))
      }

      "return true if 2 dependency is a retrospective update, and other is not" in {
        val dep1 = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false, isPassive = false)
        val dep2 = MetastoreDependency("table2" :: Nil, "@infoDate", Some("@infoDate"), triggerUpdates = true, isOptional = false, isPassive = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table3", date, date))
          .thenReturn(Some(DataChunk("table3", date.toString, date.toString, date.toString, 100, 100, 20000, 21000)))

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 10000, 11000)))

        when(bk.getLatestDataChunk("table2", date, date))
          .thenReturn(Some(DataChunk("table2", date.toString, date.toString, date.toString, 100, 100, 30000, 31000)))

        assert(anyDependencyUpdatedRetrospectively("table3", date, dep1 :: dep2 :: Nil, bk))
      }

      "return false if the dependency that has retrospective updates is not tracked" in {
        val dep1 = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false, isPassive = false)
        val dep2 = MetastoreDependency("table2" :: Nil, "@infoDate", Some("@infoDate"), triggerUpdates = false, isOptional = false, isPassive = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table3", date, date))
          .thenReturn(Some(DataChunk("table3", date.toString, date.toString, date.toString, 100, 100, 20000, 21000)))

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 10000, 11000)))

        assert(!anyDependencyUpdatedRetrospectively("table3", date, dep1 :: dep2 :: Nil, bk))
      }
    }

    "isDependencyUpdatedRetrospectively" should {
      "return false if the dependency if the job hasn't been ran" in {
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false, isPassive = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table2", date, date)).thenReturn(None)

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 10000, 11000)))

        assert(!isDependencyUpdatedRetrospectively("table2", date, dep, bk))
      }

      "return false if the dependency if the output table is not updated retrospectively" in {
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false, isPassive = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table2", date, date))
          .thenReturn(Some(DataChunk("table2", date.toString, date.toString, date.toString, 100, 100, 20000, 21000)))

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 10000, 11000)))

        assert(!isDependencyUpdatedRetrospectively("table2", date, dep, bk))
      }

      "return true if the dependency if the output table is updated retrospectively" in {
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false, isPassive = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table2", date, date))
          .thenReturn(Some(DataChunk("table2", date.toString, date.toString, date.toString, 100, 100, 20000, 21000)))

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 30000, 31000)))

        assert(isDependencyUpdatedRetrospectively("table2", date, dep, bk))
      }

      "return false if tracking updates is disabled for the dependency" in {
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = false, isOptional = false, isPassive = false)
        val bk = mock(classOf[Bookkeeper])

        assert(!isDependencyUpdatedRetrospectively("table2", date, dep, bk))
      }
    }

    "getInfoDateRange" should {
      val date = LocalDate.of(2022, 2, 18)
      val schedule = Schedule.EveryDay()

      "return a range for a one day" in {
        assert(getInfoDateRange(date, date, "@runDate", schedule) == List(date))
      }

      "return a range for several days" in {
        assert(getInfoDateRange(date, date.plusDays(3), "@runDate", schedule) ==
          List(date, date.plusDays(1), date.plusDays(2), date.plusDays(3)))
      }

      "return nothing if the upper bound is less than the lower bound" in {
        assert(getInfoDateRange(date, date.minusDays(1), "@runDate", schedule) == Nil )
      }

      "return 3 Saturdays for a weekly range" in {
        assert(getInfoDateRange(date, date.plusDays(14), "lastSaturday(@runDate)", schedule) ==
          List(date.minusDays(6), date.plusDays(1), date.plusDays(8)))
      }
    }

    "evaluateRunDate" should {
      val runDate = LocalDate.of(2022, 2, 18)

      "evaluate an info date from another info date 1" in {
        val expectedOutput = LocalDate.of(2022, 2, 19)

        assert(evaluateRunDate(runDate, "@runDate + 1") == expectedOutput)
      }

      "evaluate an info date from another info date 2" in {
        val expectedOutput = LocalDate.of(2022, 2, 17)

        assert(evaluateRunDate(runDate, "@date - 1") == expectedOutput)
      }

      "throw an exception is a run date is used" in {
        val ex = intercept[SyntaxErrorException] {
          evaluateRunDate(runDate, "@infoDate + 1")
        }

        assert(ex.getMessage.contains("Unset variable 'infoDate' used."))
      }
    }

    "evaluateFromInfoDate" should {
      val infoDate = LocalDate.of(2022, 2, 18)

      "evaluate an info date from another info date" in {
        val expectedOutput = LocalDate.of(2022, 2, 19)

        assert(evaluateFromInfoDate(infoDate, "@infoDate + 1") == expectedOutput)
      }

      "throw an exception is a run date is used" in {
        val ex = intercept[SyntaxErrorException] {
          evaluateFromInfoDate(infoDate, "@runDate + 1")
        }

        assert(ex.getMessage.contains("Unset variable 'runDate' used."))
      }
    }

    "renderPeriod" should {
      val dateFrom = LocalDate.of(2022, 2, 18)
      val dateTo = LocalDate.of(2022, 2, 25)

      "render empty range" in {
        assert(renderPeriod(None, None) == "")
      }
      "left half interval" in {
        assert(renderPeriod(Some(dateFrom), None) == " (from 2022-02-18)")
      }
      "right half interval" in {
        assert(renderPeriod(None, Some(dateTo)) == " (up to 2022-02-25)")
      }
      "full interval" in {
        assert(renderPeriod(Some(dateFrom), Some(dateTo)) == " (from 2022-02-18 to 2022-02-25)")
      }
    }
  }

  "getNextExpectedInfoDate" should {
    "correctly work with daily dates" in {
      val schedule = Schedule.EveryDay()
      val infoDateExpression = "minusDays(@runDate,1)"

      val nextExpected = getNextExpectedInfoDate(LocalDate.parse("2023-11-05"), infoDateExpression, schedule)

      assert(nextExpected == LocalDate.parse("2023-11-06"))
    }

    "correctly work with weekly dates" in {
      val schedule = Schedule.Weekly(Seq(DayOfWeek.MONDAY))
      val infoDateExpression = "minusDays(@runDate,1)"

      val nextExpected = getNextExpectedInfoDate(LocalDate.parse("2023-11-05"), infoDateExpression, schedule)

      assert(nextExpected == LocalDate.parse("2023-11-12"))
    }

    "correctly work with weekly dates and more complicated info date formula" in {
      val schedule = Schedule.Weekly(Seq(DayOfWeek.MONDAY))
      val infoDateExpression = "lastSaturday(@runDate)"

      val nextExpected = getNextExpectedInfoDate(LocalDate.parse("2023-11-04"), infoDateExpression, schedule)

      assert(nextExpected == LocalDate.parse("2023-11-11"))
    }

    "correctly work with monthly dates" in {
      val schedule = Schedule.Monthly(Seq(1))
      val infoDateExpression = "minusDays(@runDate,1)"

      val nextExpected = getNextExpectedInfoDate(LocalDate.parse("2023-11-01"), infoDateExpression, schedule)

      assert(nextExpected == LocalDate.parse("2023-11-30"))
    }

    "correctly work with schedules that never enabled" in {
      val schedule = Schedule.Monthly(Seq.empty)
      val infoDateExpression = "minusDays(@runDate,1)"

      val nextExpected = getNextExpectedInfoDate(LocalDate.parse("2023-11-01"), infoDateExpression, schedule)

      assert(nextExpected == LocalDate.parse("2023-11-02"))
    }

    "correctly work with dates that increase with info date" in {
      val schedule = Schedule.EveryDay()
      val infoDateExpression = "plusDays(@runDate,1)"

      val nextExpected = getNextExpectedInfoDate(LocalDate.parse("2023-11-05"), infoDateExpression, schedule)

      assert(nextExpected == LocalDate.parse("2023-11-07"))
    }

    "correctly work with an out of schedule example" in {
      val schedule = Schedule.Weekly(Seq(DayOfWeek.SUNDAY))
      val infoDateExpression = "lastSaturday(@runDate)"

      // 2022-07-03. 2022-07-10 - Sundays
      val nextExpected = getNextExpectedInfoDate(LocalDate.parse("2022-07-05"), infoDateExpression, schedule)

      assert(nextExpected == LocalDate.parse("2022-07-09"))
    }
  }

  "getMinRunDateFromInfoDate" should {
    "return correct date for a daily schedule" in {
      val schedule = Schedule.EveryDay()

      val expected = LocalDate.parse("2023-11-12")

      val actual = getMinRunDateFromInfoDate(LocalDate.parse("2023-11-12"), schedule)

      assert(actual == expected)
    }

    "return correct date for a weekly schedule" in {
      val schedule = Schedule.Weekly(Seq(DayOfWeek.MONDAY))

      val expected = LocalDate.parse("2023-11-06")

      val actual = getMinRunDateFromInfoDate(LocalDate.parse("2023-11-12"), schedule)

      assert(actual == expected)
    }

    "return correct date for a monthly schedule" in {
      val schedule = Schedule.Monthly(Seq(1))

      val expected = LocalDate.parse("2023-11-01")

      val actual = getMinRunDateFromInfoDate(LocalDate.parse("2023-11-12"), schedule)

      assert(actual == expected)

    }
  }

  "getActiveInfoDates" should {
    "return correct dates for a daily schedule" in {
      val schedule = Schedule.EveryDay()
      val infoDateExpression = "minusDays(@runDate,1)"

      val expected = List(
        LocalDate.parse("2023-11-12"),
        LocalDate.parse("2023-11-13"),
        LocalDate.parse("2023-11-14"),
        LocalDate.parse("2023-11-15")
      )

      val actual = getActiveInfoDates("dummy_table", LocalDate.parse("2023-11-12"), LocalDate.parse("2023-11-15"), infoDateExpression, schedule)

      assert(actual == expected)
    }

    "return correct dates for a weekly schedule" in {
      val schedule = Schedule.Weekly(Seq(DayOfWeek.MONDAY))
      val infoDateExpression = "lastSunday(@runDate)"

      val expected = List(
        LocalDate.parse("2023-11-12"),
        LocalDate.parse("2023-11-19"),
        LocalDate.parse("2023-11-26"),
        LocalDate.parse("2023-12-03")
      )

      val actual = getActiveInfoDates("dummy_table", LocalDate.parse("2023-11-11"), LocalDate.parse("2023-12-04"), infoDateExpression, schedule)

      assert(actual == expected)
    }

    "return correct dates for a monthly schedule" in {
      val schedule = Schedule.Monthly(Seq(1))
      val infoDateExpression = "minusDays(@runDate,1)"

      val expected = List(
        LocalDate.parse("2023-11-30"),
        LocalDate.parse("2023-12-31"),
        LocalDate.parse("2024-01-31"),
        LocalDate.parse("2024-02-29")
      )

      val actual = getActiveInfoDates("dummy_table", LocalDate.parse("2023-11-28"), LocalDate.parse("2024-03-01"), infoDateExpression, schedule)

      assert(actual == expected)
    }

    "throw an exception for forward looking info date expression" in {
      val schedule = Schedule.EveryDay()
      val infoDateExpression = "plusDays(@runDate,1)"

      val ex = intercept[IllegalArgumentException] {
        getActiveInfoDates("dummy_table", LocalDate.parse("2023-11-12"), LocalDate.parse("2023-11-15"), infoDateExpression, schedule)
      }

      assert(ex.getMessage.contains(s"Could not use forward looking info date expression (plusDays(@runDate,1)) for the table 'dummy_table'."))

    }
  }

  "getLatestActiveInfoDate" should {
    "return correct date for a daily schedule" in {
      val schedule = Schedule.EveryDay()
      val infoDateExpression = "minusDays(@runDate,1)"

      val expected = LocalDate.parse("2023-11-15")

      val actual = getLatestActiveInfoDate("dummy_table", LocalDate.parse("2023-11-15"), infoDateExpression, schedule)

      assert(actual == expected)
    }

    "return correct date for a weekly schedule" in {
      val schedule = Schedule.Weekly(Seq(DayOfWeek.MONDAY))
      val infoDateExpression = "lastSunday(@runDate)"

      val expected = LocalDate.parse("2023-12-03")

      val actual = getLatestActiveInfoDate("dummy_table", LocalDate.parse("2023-12-05"), infoDateExpression, schedule)

      assert(actual == expected)
    }

    "return correct date for a monthly schedule" in {
      val schedule = Schedule.Monthly(Seq(1))
      val infoDateExpression = "minusDays(@runDate,1)"

      val expected = LocalDate.parse("2024-02-29")

      val actual = getLatestActiveInfoDate("dummy_table", LocalDate.parse("2024-03-20"), infoDateExpression, schedule)

      assert(actual == expected)
    }

    "throw an exception for forward looking info date expression" in {
      val schedule = Schedule.EveryDay()
      val infoDateExpression = "plusDays(@runDate,1)"

      val ex = intercept[IllegalArgumentException] {
        getLatestActiveInfoDate("dummy_table", LocalDate.parse("2023-11-15"), infoDateExpression, schedule)
      }

      assert(ex.getMessage.contains(s"Could not use forward looking info date expression (plusDays(@runDate,1)) for the table 'dummy_table'."))
    }
  }
}
