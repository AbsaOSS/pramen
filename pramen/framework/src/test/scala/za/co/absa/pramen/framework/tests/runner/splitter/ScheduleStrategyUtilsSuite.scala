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

package za.co.absa.pramen.framework.tests.runner.splitter

import org.mockito.Mockito.{mock, when}
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.bookkeeper.Bookkeeper
import za.co.absa.pramen.framework.expr.exceptions.SyntaxErrorException
import za.co.absa.pramen.framework.metastore.model.MetastoreDependency
import za.co.absa.pramen.framework.pipeline
import za.co.absa.pramen.framework.pipeline.{TaskPreDef, TaskRunReason}
import za.co.absa.pramen.framework.model.DataChunk
import za.co.absa.pramen.framework.runner.splitter.RunMode
import za.co.absa.pramen.framework.runner.splitter.ScheduleStrategyUtils._
import za.co.absa.pramen.framework.schedule.Schedule

import java.time.{DayOfWeek, LocalDate}

class ScheduleStrategyUtilsSuite extends WordSpec {
  "ScheduleStrategyCommon" when {
    val date = LocalDate.of(2022, 2, 18)

    "getRerun" should {
      "return information date of the rerun" in {
        val expected = pipeline.TaskPreDef(date.minusDays(1), TaskRunReason.Rerun)

        assert(getRerun("table", date, "@runDate - 1") == expected :: Nil)
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
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestProcessedDate("table")).thenReturn(None)

        val expected = List(date.minusDays(3), date.minusDays(2), date.minusDays(1))
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))

        val actual = getLate("table", date, Schedule.EveryDay(), "@runDate", date.minusDays(3), bk)

        assert(actual == expected)
      }

      "return the date range when there is some late data" in {
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestProcessedDate("table")).thenReturn(Some(date.minusDays(2)))

        val expected = List(date.minusDays(1))
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))

        val actual = getLate("table", date, Schedule.EveryDay(), "@runDate", date.minusDays(3), bk)

        assert(actual == expected)
      }

      "return empty list when data is up to date" in {
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestProcessedDate("table")).thenReturn(Some(date.minusDays(1)))

        val actual = getLate("table", date, Schedule.EveryDay(), "@runDate", date.minusDays(3), bk)

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
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table2", date, date))
          .thenReturn(Some(DataChunk("table2", date.toString, date.toString, date.toString, 100, 100, 20000, 21000)))

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 10000, 11000)))

        assert(!anyDependencyUpdatedRetrospectively("table2", date, dep :: Nil, bk))
      }

      "return true if a dependency is specified that has retrospective updates" in {
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table2", date, date))
          .thenReturn(Some(DataChunk("table2", date.toString, date.toString, date.toString, 100, 100, 20000, 21000)))

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 30000, 31000)))

        assert(anyDependencyUpdatedRetrospectively("table2", date, dep :: Nil, bk))
      }

      "return true if 2 dependency is a retrospective update, and other is not" in {
        val dep1 = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false)
        val dep2 = MetastoreDependency("table2" :: Nil, "@infoDate", Some("@infoDate"), triggerUpdates = true, isOptional = false)
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
        val dep1 = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false)
        val dep2 = MetastoreDependency("table2" :: Nil, "@infoDate", Some("@infoDate"), triggerUpdates = false, isOptional = false)
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
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table2", date, date)).thenReturn(None)

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 10000, 11000)))

        assert(!isDependencyUpdatedRetrospectively("table2", date, dep, bk))
      }

      "return false if the dependency if the output table is not updated retrospectively" in {
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table2", date, date))
          .thenReturn(Some(DataChunk("table2", date.toString, date.toString, date.toString, 100, 100, 20000, 21000)))

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 10000, 11000)))

        assert(!isDependencyUpdatedRetrospectively("table2", date, dep, bk))
      }

      "return true if the dependency if the output table is updated retrospectively" in {
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false)
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestDataChunk("table2", date, date))
          .thenReturn(Some(DataChunk("table2", date.toString, date.toString, date.toString, 100, 100, 20000, 21000)))

        when(bk.getLatestDataChunk("table1", date.minusDays(7), date))
          .thenReturn(Some(DataChunk("table1", date.toString, date.toString, date.toString, 100, 100, 30000, 31000)))

        assert(isDependencyUpdatedRetrospectively("table2", date, dep, bk))
      }

      "return false if tracking updates is disabled for the dependency" in {
        val dep = MetastoreDependency("table1" :: Nil, "@infoDate - 7", Some("@infoDate"), triggerUpdates = false, isOptional = false)
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
}
