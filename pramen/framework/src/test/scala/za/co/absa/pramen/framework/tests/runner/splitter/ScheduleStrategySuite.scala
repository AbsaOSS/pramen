/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import za.co.absa.pramen.api.schedule.{EveryDay, Weekly}
import za.co.absa.pramen.api.v2.MetastoreDependency
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.pipeline
import za.co.absa.pramen.framework.pipeline.{TaskPreDef, TaskRunReason}
import za.co.absa.pramen.framework.mocks.DataChunkFactory.getDummyDataChunk
import za.co.absa.pramen.framework.runner.splitter.{RunMode, ScheduleParams, ScheduleStrategySourcing, ScheduleStrategyTransformation}

import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate}
import scala.language.implicitConversions

class ScheduleStrategySuite extends WordSpec {
  implicit private def toDate(str: String): LocalDate = {
    LocalDate.parse(str, DateTimeFormatter.ISO_LOCAL_DATE)
  }

  "ScheduleStrategySourcing" when {
    val outputTable = "output_table"
    val dependencies = Seq.empty[MetastoreDependency]
    val runDate = LocalDate.of(2022, 2, 18)
    val minimumDate = LocalDate.of(2022, 2, 1)
    val strategy = new ScheduleStrategySourcing()

    "daily" when {
      val infoDateExpression = "@runDate"
      val schedule = EveryDay()

      "normal execution" in {
        val bk = mock(classOf[SyncBookKeeper])

        when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(2)))

        val params = ScheduleParams.Normal(runDate, 4, 0, newOnly = false, lateOnly = false)

        val expected = Seq(
          pipeline.TaskPreDef(runDate.minusDays(4), TaskRunReason.Late),
          pipeline.TaskPreDef(runDate.minusDays(3), TaskRunReason.Late),
          pipeline.TaskPreDef(runDate.minusDays(2), TaskRunReason.Late),
          pipeline.TaskPreDef(runDate.minusDays(1), TaskRunReason.Late),
          pipeline.TaskPreDef(runDate, TaskRunReason.New)
        )

        val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

        assert(result == expected)
      }

      "late only" in {
        val bk = mock(classOf[SyncBookKeeper])

        when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(2)))

        val params = ScheduleParams.Normal(runDate, 4, 0, newOnly = false, lateOnly = true)

        val expected = Seq(runDate.minusDays(1))
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))

        val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

        assert(result == expected)
      }

      "new only" in {
        val bk = mock(classOf[SyncBookKeeper])

        when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(2)))

        val params = ScheduleParams.Normal(runDate, 4, 0, newOnly = true, lateOnly = false)

        val expected = Seq(runDate)
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

        val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

        assert(result == expected)
      }

      "incorrect settings" in {
        val bk = mock(classOf[SyncBookKeeper])

        when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(2)))

        val params = ScheduleParams.Normal(runDate, 4, 0, newOnly = true, lateOnly = true)

        val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

        assert(result.isEmpty)
      }

      "rerun" when {
        "normal rerun" in {
          val bk = mock(classOf[SyncBookKeeper])
          val infoDateExpression = "@runDate - 2"

          when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(2)))

          val params = ScheduleParams.Rerun(runDate.minusDays(5))

          val expected = Seq(pipeline.TaskPreDef(runDate.minusDays(7), TaskRunReason.Rerun))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "earlier than the minimum date" in {
          val bk = mock(classOf[SyncBookKeeper])

          when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(2)))

          val params = ScheduleParams.Rerun(runDate.minusDays(365))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result.isEmpty)
        }
      }

      "historical" when {
        val bk = mock(classOf[SyncBookKeeper])
        when(bk.getDataChunksCount(outputTable, Some(runDate.minusDays(3)), Some(runDate.minusDays(3)))).thenReturn(100)

        "fill gaps" in {
          val params = ScheduleParams.Historical(runDate.minusDays(5), runDate.minusDays(1), inverseDateOrder = false, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(runDate.minusDays(5),
            runDate.minusDays(4),
            runDate.minusDays(2),
            runDate.minusDays(1))
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "rerun all" in {
          val params = ScheduleParams.Historical(runDate.minusDays(5), runDate.minusDays(1), inverseDateOrder = false, mode = RunMode.ForceRun)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          val expected = Seq(pipeline.TaskPreDef(runDate.minusDays(5), TaskRunReason.New),
            pipeline.TaskPreDef(runDate.minusDays(4), TaskRunReason.New),
            pipeline.TaskPreDef(runDate.minusDays(3), TaskRunReason.Rerun),
            pipeline.TaskPreDef(runDate.minusDays(2), TaskRunReason.New),
            pipeline.TaskPreDef(runDate.minusDays(1), TaskRunReason.New)
          )

          assert(result == expected)
        }

        "reverse order" in {
          val params = ScheduleParams.Historical(runDate.minusDays(5), runDate.minusDays(1), inverseDateOrder = true, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(runDate.minusDays(1),
            runDate.minusDays(2),
            runDate.minusDays(4),
            runDate.minusDays(5))
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }
      }
    }

    "weekly" when {
      val infoDateExpression = "lastSaturday(@date)"
      val schedule = Weekly(DayOfWeek.SUNDAY :: Nil)

      val saturdayTwoWeeksAgo = runDate.minusDays(13)
      val lastSaturday = runDate.minusDays(6)
      val nextSaturday = runDate.plusDays(1)
      val nextSunday = runDate.plusDays(2)

      "normal execution" when {
        val bk = mock(classOf[SyncBookKeeper])
        when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(9)))

        "default behavior" in {
          val params = ScheduleParams.Normal(nextSunday, 14, 0, newOnly = false, lateOnly = false)

          val expected = Seq(
            pipeline.TaskPreDef(saturdayTwoWeeksAgo, TaskRunReason.Late),
            pipeline.TaskPreDef(lastSaturday, TaskRunReason.Late),
            pipeline.TaskPreDef(nextSaturday, TaskRunReason.New)
          )

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "late only" in {
          val params = ScheduleParams.Normal(nextSunday, 14, 0, newOnly = false, lateOnly = true)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == Seq(pipeline.TaskPreDef(lastSaturday, TaskRunReason.Late)))
        }

        "new only" in {
          val params = ScheduleParams.Normal(nextSunday, 14, 0, newOnly = true, lateOnly = false)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == Seq(pipeline.TaskPreDef(nextSaturday, TaskRunReason.New)))
        }

        "incorrect settings" in {
          val params = ScheduleParams.Normal(runDate, 4, 0, newOnly = true, lateOnly = true)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result.isEmpty)
        }
      }

      "rerun" when {
        val bk = mock(classOf[SyncBookKeeper])
        val infoDateExpression = "@runDate - 2"

        when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(9)))

        "normal rerun" in {
          val params = ScheduleParams.Rerun(runDate.minusDays(5))

          val expected = Seq(pipeline.TaskPreDef(runDate.minusDays(7), TaskRunReason.Rerun))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "earlier than the minimum date" in {
          val params = ScheduleParams.Rerun(runDate.minusDays(365))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result.isEmpty)
        }
      }

      "historical" when {
        val bk = mock(classOf[SyncBookKeeper])
        when(bk.getDataChunksCount(outputTable, Some(lastSaturday), Some(lastSaturday))).thenReturn(100)

        "fill gaps" in {
          val params = ScheduleParams.Historical(runDate.minusDays(14), nextSunday, inverseDateOrder = false, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(saturdayTwoWeeksAgo,
            nextSaturday)
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "rerun all" in {
          val params = ScheduleParams.Historical(runDate.minusDays(14), nextSunday, inverseDateOrder = false, mode = RunMode.ForceRun)

          val expected = Seq(pipeline.TaskPreDef(saturdayTwoWeeksAgo, TaskRunReason.New),
            pipeline.TaskPreDef(lastSaturday, TaskRunReason.Rerun),
            pipeline.TaskPreDef(nextSaturday, TaskRunReason.New)
          )

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "reverse order" in {
          val params = ScheduleParams.Historical(runDate.minusDays(14), nextSunday, inverseDateOrder = true, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(nextSaturday,
            saturdayTwoWeeksAgo)
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }
      }
    }
  }

  "ScheduleStrategyTransformation" when {
    val outputTable = "output_table"
    val dependencies = Seq(MetastoreDependency(Seq("table1"), "@infoDate - 7", Some("@infoDate"), triggerUpdates = true, isOptional = false))
    val runDate = LocalDate.of(2022, 2, 18)
    val minimumDate = LocalDate.of(2022, 2, 1)
    val strategy = new ScheduleStrategyTransformation()

    "daily" when {
      val infoDateExpression = "@runDate"
      val schedule = EveryDay()

      "normal setup" when {
        val bk = mock(classOf[SyncBookKeeper])

        when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(2)))

        // Output table bookkeeping mocks
        val dc14 = getDummyDataChunk(outputTable, "2022-02-14")
        val dc15 = getDummyDataChunk(outputTable, "2022-02-15")

        when(bk.getLatestDataChunk(outputTable, "2022-02-14", "2022-02-14")).thenReturn(Some(dc14))
        when(bk.getLatestDataChunk(outputTable, "2022-02-15", "2022-02-15")).thenReturn(Some(dc15))
        when(bk.getLatestDataChunk(outputTable, "2022-02-16", "2022-02-16")).thenReturn(None)
        when(bk.getLatestDataChunk(outputTable, "2022-02-17", "2022-02-17")).thenReturn(None)

        // Dependencies (input tables) bookkeeping mocks
        val dc12 = getDummyDataChunk("table1", "2022-02-12", jobFinished = 15000)

        when(bk.getLatestDataChunk("table1", "2022-02-07", "2022-02-14")).thenReturn(Some(dc12))
        when(bk.getLatestDataChunk("table1", "2022-02-08", "2022-02-15")).thenReturn(Some(dc12))

        "normal execution" in {
          val params = ScheduleParams.Normal(runDate, 4, 0, newOnly = false, lateOnly = false)

          val expected = Seq(pipeline.TaskPreDef(toDate("2022-02-17"), TaskRunReason.Late),
              pipeline.TaskPreDef(toDate("2022-02-18"), TaskRunReason.New))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "late only" in {
          val params = ScheduleParams.Normal(runDate, 4, 0, newOnly = false, lateOnly = true)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == Seq(pipeline.TaskPreDef(toDate("2022-02-17"), TaskRunReason.Late)))
        }

        "new only" in {
          val params = ScheduleParams.Normal(runDate, 4, 0, newOnly = true, lateOnly = false)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == Seq(pipeline.TaskPreDef(runDate, TaskRunReason.New)))
        }

        "incorrect settings" in {
          val params = ScheduleParams.Normal(runDate, 4, 0, newOnly = true, lateOnly = true)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result.isEmpty)
        }

        "retrospective updates" in {
          val dc14 = getDummyDataChunk(outputTable, "2022-02-14", jobFinished = 5000)

          when(bk.getLatestDataChunk(outputTable, "2022-02-14", "2022-02-14")).thenReturn(Some(dc14))

          val params = ScheduleParams.Normal(runDate, 4, 0, newOnly = false, lateOnly = false)

          val expected = Seq(pipeline.TaskPreDef(toDate("2022-02-14"), TaskRunReason.Update),
            pipeline.TaskPreDef(toDate("2022-02-17"), TaskRunReason.Late),
            pipeline.TaskPreDef(toDate("2022-02-18"), TaskRunReason.New))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }
      }

      "rerun" when {
        "normal rerun" in {
          val bk = mock(classOf[SyncBookKeeper])
          val infoDateExpression = "@runDate - 2"

          when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(2)))

          val params = ScheduleParams.Rerun(runDate.minusDays(5))

          val expected = Seq(pipeline.TaskPreDef(runDate.minusDays(7), TaskRunReason.Rerun))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "earlier than the minimum date" in {
          val bk = mock(classOf[SyncBookKeeper])

          when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(2)))

          val params = ScheduleParams.Rerun(runDate.minusDays(365))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result.isEmpty)
        }
      }

      "historical" when {
        val bk = mock(classOf[SyncBookKeeper])
        when(bk.getDataChunksCount(outputTable, Some(runDate.minusDays(3)), Some(runDate.minusDays(3)))).thenReturn(100)

        "fill gaps" in {
          val params = ScheduleParams.Historical(runDate.minusDays(5), runDate.minusDays(1), inverseDateOrder = false, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(runDate.minusDays(5),
            runDate.minusDays(4),
            runDate.minusDays(2),
            runDate.minusDays(1))
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "rerun all" in {
          val params = ScheduleParams.Historical(runDate.minusDays(5), runDate.minusDays(1), inverseDateOrder = false, mode = RunMode.ForceRun)

          val expected = Seq(pipeline.TaskPreDef(runDate.minusDays(5), TaskRunReason.New),
            pipeline.TaskPreDef(runDate.minusDays(4), TaskRunReason.New),
            pipeline.TaskPreDef(runDate.minusDays(3), TaskRunReason.Rerun),
            pipeline.TaskPreDef(runDate.minusDays(2), TaskRunReason.New),
            pipeline.TaskPreDef(runDate.minusDays(1), TaskRunReason.New)
          )

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "reverse order" in {
          val params = ScheduleParams.Historical(runDate.minusDays(5), runDate.minusDays(1), inverseDateOrder = true, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(runDate.minusDays(1),
            runDate.minusDays(2),
            runDate.minusDays(4),
            runDate.minusDays(5))
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }
      }
    }

    "weekly" when {
      val infoDateExpression = "lastSaturday(@date)"
      val schedule = Weekly(DayOfWeek.SUNDAY :: Nil)

      val saturdayTwoWeeksAgo = runDate.minusDays(13)
      val lastSaturday = runDate.minusDays(6)
      val nextSaturday = runDate.plusDays(1)
      val nextSunday = runDate.plusDays(2)

      "normal setup" when {
        val bk = mock(classOf[SyncBookKeeper])

        when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(2)))

        // Output table bookkeeping mocks
        val dc5o = getDummyDataChunk(outputTable, "2022-02-05")
        val dc12o = getDummyDataChunk(outputTable, "2022-02-12")

        when(bk.getLatestDataChunk(outputTable, "2022-02-05", "2022-02-05")).thenReturn(Some(dc5o))
        when(bk.getLatestDataChunk(outputTable, "2022-02-12", "2022-02-12")).thenReturn(Some(dc12o))
        when(bk.getLatestDataChunk(outputTable, "2022-02-16", "2022-02-16")).thenReturn(None)
        when(bk.getLatestDataChunk(outputTable, "2022-02-17", "2022-02-17")).thenReturn(None)

        // Dependencies (input tables) bookkeeping mocks
        val dc5 = getDummyDataChunk("table1", "2022-02-05", jobFinished = 15000)
        val dc12 = getDummyDataChunk("table1", "2022-02-12", jobFinished = 15000)

        when(bk.getLatestDataChunk("table1", "2022-01-29", "2022-02-05")).thenReturn(Some(dc5))
        when(bk.getLatestDataChunk("table1", "2022-02-05", "2022-02-12")).thenReturn(Some(dc12))

        "normal execution" in {
          val params = ScheduleParams.Normal(nextSunday, 14, 0, newOnly = false, lateOnly = false)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == Seq(pipeline.TaskPreDef(toDate("2022-02-19"), TaskRunReason.New)))
        }

        "late only" in {
          when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(8)))

          val params = ScheduleParams.Normal(nextSunday, 14, 0, newOnly = false, lateOnly = true)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == Seq(pipeline.TaskPreDef(toDate("2022-02-12"), TaskRunReason.Late)))
        }

        "new only" in {
          val params = ScheduleParams.Normal(nextSunday, 14, 0, newOnly = true, lateOnly = false)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == Seq(pipeline.TaskPreDef(toDate("2022-02-19"), TaskRunReason.New)))
        }

        "incorrect settings" in {
          val params = ScheduleParams.Normal(nextSunday, 14, 0, newOnly = true, lateOnly = true)

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result.isEmpty)
        }

        "retrospective updates" in {
          val dc5 = getDummyDataChunk("table1", "2022-02-05", jobFinished = 30000)

          when(bk.getLatestDataChunk("table1", "2022-01-29", "2022-02-05")).thenReturn(Some(dc5))
          when(bk.getLatestDataChunk("table1", "2022-02-05", "2022-02-12")).thenReturn(Some(dc12))

          val params = ScheduleParams.Normal(nextSunday, 14, 0, newOnly = false, lateOnly = false)

          val expected = Seq(
            pipeline.TaskPreDef(toDate("2022-02-05"), TaskRunReason.Update),
            pipeline.TaskPreDef(toDate("2022-02-12"), TaskRunReason.Late),
            pipeline.TaskPreDef(toDate("2022-02-19"), TaskRunReason.New)
          )

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }
      }

      "rerun" when {
        val bk = mock(classOf[SyncBookKeeper])
        val infoDateExpression = "@runDate - 2"

        when(bk.getLatestProcessedDate(outputTable)).thenReturn(Some(runDate.minusDays(9)))

        "normal rerun" in {
          val params = ScheduleParams.Rerun(runDate.minusDays(5))

          val expected = Seq(pipeline.TaskPreDef(runDate.minusDays(7), TaskRunReason.Rerun))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "earlier than the minimum date" in {
          val params = ScheduleParams.Rerun(runDate.minusDays(365))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result.isEmpty)
        }
      }

      "historical" when {
        val bk = mock(classOf[SyncBookKeeper])
        when(bk.getDataChunksCount(outputTable, Some(lastSaturday), Some(lastSaturday))).thenReturn(100)

        "fill gaps" in {
          val params = ScheduleParams.Historical(runDate.minusDays(14), nextSunday, inverseDateOrder = false, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(saturdayTwoWeeksAgo,
            nextSaturday)
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "rerun all" in {
          val params = ScheduleParams.Historical(runDate.minusDays(14), nextSunday, inverseDateOrder = false, mode = RunMode.ForceRun)

          val expected = Seq(pipeline.TaskPreDef(saturdayTwoWeeksAgo, TaskRunReason.New),
            pipeline.TaskPreDef(lastSaturday, TaskRunReason.Rerun),
            pipeline.TaskPreDef(nextSaturday, TaskRunReason.New)
          )

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }

        "reverse order" in {
          val params = ScheduleParams.Historical(runDate.minusDays(14), nextSunday, inverseDateOrder = true, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(nextSaturday, saturdayTwoWeeksAgo)
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategy.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, minimumDate)

          assert(result == expected)
        }
      }
    }
  }
}
