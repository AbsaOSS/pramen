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

import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.RunMode
import za.co.absa.pramen.api.jobdef.Schedule
import za.co.absa.pramen.api.status.{MetastoreDependency, TaskRunReason}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.pipeline
import za.co.absa.pramen.core.runner.splitter.{ScheduleParams, ScheduleStrategySourcing}

import java.time.{DayOfWeek, LocalDate}
import scala.language.implicitConversions

class ScheduleStrategySuite extends AnyWordSpec {
  "ScheduleStrategySourcing" when {
    val outputTable = "output_table"
    val dependencies = Seq.empty[MetastoreDependency]
    val runDate = LocalDate.of(2022, 2, 18)
    val minimumDate = LocalDate.of(2022, 2, 1)
    val initialSourcingDateExpr = "@runDate - 2"
    val strategyEvent = new ScheduleStrategySourcing(true)
    val strategySnapshot = new ScheduleStrategySourcing(false)

    "daily" when {
      val infoDateExpression = "@runDate"
      val schedule = Schedule.EveryDay()

      "normal execution" in {
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestProcessedDate(outputTable, Some(runDate))).thenReturn(Some(runDate.minusDays(2)))

        val params = ScheduleParams.Normal(runDate, 0, 4, 0, newOnly = false, lateOnly = false)

        val expected = Seq(
          pipeline.TaskPreDef(runDate.minusDays(3), TaskRunReason.Late),
          pipeline.TaskPreDef(runDate.minusDays(2), TaskRunReason.Late),
          pipeline.TaskPreDef(runDate.minusDays(1), TaskRunReason.Late),
          pipeline.TaskPreDef(runDate, TaskRunReason.New)
        )

        val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

        assert(result == expected)
      }

      "normal execution with zero track days" in {
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestProcessedDate(outputTable, Some(runDate.minusDays(1)))).thenReturn(Some(runDate.minusDays(1)))

        val params = ScheduleParams.Normal(runDate, 0, 0, 0, newOnly = false, lateOnly = false)

        val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, "@runDate - 1", schedule, params, initialSourcingDateExpr, minimumDate)

        assert(result.isEmpty)
      }

      "normal execution with snapshot table and job ran sometime ago" in {
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestProcessedDate(outputTable, Some(runDate.minusDays(1)))).thenReturn(Some(runDate.minusDays(7)))

        val params = ScheduleParams.Normal(runDate, 0, 0, 0, newOnly = false, lateOnly = false)

        val result = strategySnapshot.getDaysToRun(outputTable, dependencies, bk, "@runDate - 1", schedule, params, initialSourcingDateExpr, minimumDate)

        assert(result.length == 1)
        assert(result.head.infoDate == runDate.minusDays(1))
        assert(result.head.reason == TaskRunReason.New)
      }

      "late only" in {
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestProcessedDate(outputTable, Some(runDate))).thenReturn(Some(runDate.minusDays(2)))

        val params = ScheduleParams.Normal(runDate, 0, 4, 0, newOnly = false, lateOnly = true)

        val expected = Seq(runDate.minusDays(1))
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.Late))

        val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

        assert(result == expected)
      }

      "new only" in {
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestProcessedDate(outputTable, Some(runDate))).thenReturn(Some(runDate.minusDays(2)))

        val params = ScheduleParams.Normal(runDate, 0, 4, 0, newOnly = true, lateOnly = false)

        val expected = Seq(runDate)
          .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

        val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

        assert(result == expected)
      }

      "incorrect settings" in {
        val bk = mock(classOf[Bookkeeper])

        when(bk.getLatestProcessedDate(outputTable, Some(runDate))).thenReturn(Some(runDate.minusDays(2)))

        val params = ScheduleParams.Normal(runDate, 0, 4, 0, newOnly = true, lateOnly = true)

        val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

        assert(result.isEmpty)
      }

      "rerun" when {
        "normal rerun" in {
          val bk = mock(classOf[Bookkeeper])
          val infoDateExpression = "@runDate - 2"

          when(bk.getLatestDataChunk(outputTable, runDate.minusDays(7))).thenReturn(Some(null))
          when(bk.getLatestProcessedDate(outputTable, Some(runDate))).thenReturn(Some(runDate.minusDays(2)))

          val params = ScheduleParams.Rerun(runDate.minusDays(5))

          val expected = Seq(pipeline.TaskPreDef(runDate.minusDays(7), TaskRunReason.Rerun))

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }

        "earlier than the minimum date" in {
          val bk = mock(classOf[Bookkeeper])

          when(bk.getLatestDataChunk(outputTable, runDate.minusDays(365))).thenReturn(None)
          when(bk.getLatestProcessedDate(outputTable, Some(runDate))).thenReturn(Some(runDate.minusDays(2)))

          val params = ScheduleParams.Rerun(runDate.minusDays(365))

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result.length == 1)
          assert(result.head.reason.isInstanceOf[TaskRunReason.Skip])
        }
      }

      "historical" when {
        val bk = mock(classOf[Bookkeeper])
        when(bk.getDataChunksCount(outputTable, Some(runDate.minusDays(3)), Some(runDate.minusDays(3)))).thenReturn(100)

        "fill gaps" in {
          val params = ScheduleParams.Historical(runDate.minusDays(5), runDate.minusDays(1), inverseDateOrder = false, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(runDate.minusDays(5),
            runDate.minusDays(4),
            runDate.minusDays(2),
            runDate.minusDays(1))
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }

        "rerun all" in {
          val params = ScheduleParams.Historical(runDate.minusDays(5), runDate.minusDays(1), inverseDateOrder = false, mode = RunMode.ForceRun)

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

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

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }
      }
    }

    "weekly" when {
      val infoDateExpression = "lastSaturday(@date)"
      val schedule = Schedule.Weekly(DayOfWeek.SUNDAY :: Nil)

      val saturdayTwoWeeksAgo = runDate.minusDays(13)
      val lastSaturday = runDate.minusDays(6)
      val nextSaturday = runDate.plusDays(1)
      val nextSunday = runDate.plusDays(2)

      "normal execution" when {
        "default behavior" in {
          val bk = mock(classOf[Bookkeeper])
          when(bk.getLatestProcessedDate(outputTable, Some(runDate.plusDays(1)))).thenReturn(Some(runDate.minusDays(9)))

          val params = ScheduleParams.Normal(nextSunday, 0, 15, 0, newOnly = false, lateOnly = false)

          val expected = Seq(
            pipeline.TaskPreDef(saturdayTwoWeeksAgo, TaskRunReason.Late),
            pipeline.TaskPreDef(lastSaturday, TaskRunReason.Late),
            pipeline.TaskPreDef(nextSaturday, TaskRunReason.New)
          )

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }

        "default behavior with track days" in {
          val minimumDate = LocalDate.parse("2022-07-01")
          val runDate = LocalDate.parse("2022-07-14")
          val params = ScheduleParams.Normal(runDate, 0, 6, 0, newOnly = false, lateOnly = false)

          val bk = mock(classOf[Bookkeeper])
          when(bk.getLatestProcessedDate(outputTable, Some(LocalDate.parse("2022-07-09")))).thenReturn(Some(LocalDate.parse("2022-07-05")))

          val expected = Seq(
            pipeline.TaskPreDef(LocalDate.of(2022, 7, 9), TaskRunReason.Late)
          )

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }

        "default behavior with more than 1 day late" in {
          val minimumDate = LocalDate.parse("2022-07-01")
          val runDate = LocalDate.parse("2022-07-14")
          val params = ScheduleParams.Normal(runDate, 0, 0, 0, newOnly = false, lateOnly = false)

          val bk = mock(classOf[Bookkeeper])
          when(bk.getLatestProcessedDate(outputTable, Some(LocalDate.parse("2022-07-09")))).thenReturn(Some(LocalDate.parse("2022-07-05")))

          val expected = Seq(
            pipeline.TaskPreDef(LocalDate.of(2022, 7, 9), TaskRunReason.Late)
          )

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }

        "default behavior with snapshot table and job ran sometime ago" in {
          val bk = mock(classOf[Bookkeeper])

          when(bk.getLatestProcessedDate(outputTable, Some(runDate.minusDays(1)))).thenReturn(Some(runDate.minusDays(30)))

          val params = ScheduleParams.Normal(runDate, 0, 0, 0, newOnly = false, lateOnly = false)

          val result = strategySnapshot.getDaysToRun(outputTable, dependencies, bk, "@runDate - 1", schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result.length == 1)
          assert(result.head.infoDate == lastSaturday)
          assert(result.head.reason == TaskRunReason.Late)
        }

        "late only" in {
          val params = ScheduleParams.Normal(nextSunday, 0, 14, 0, newOnly = false, lateOnly = true)

          val bk = mock(classOf[Bookkeeper])
          when(bk.getLatestProcessedDate(outputTable, Some(runDate.plusDays(1)))).thenReturn(Some(runDate.minusDays(9)))

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == Seq(pipeline.TaskPreDef(lastSaturday, TaskRunReason.Late)))
        }

        "new only" in {
          val params = ScheduleParams.Normal(nextSunday, 0, 14, 0, newOnly = true, lateOnly = false)

          val bk = mock(classOf[Bookkeeper])
          when(bk.getLatestProcessedDate(outputTable, Some(runDate.plusDays(1)))).thenReturn(Some(runDate.minusDays(9)))

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == Seq(pipeline.TaskPreDef(nextSaturday, TaskRunReason.New)))
        }

        "incorrect settings" in {
          val params = ScheduleParams.Normal(runDate, 0, 4, 0, newOnly = true, lateOnly = true)

          val bk = mock(classOf[Bookkeeper])
          when(bk.getLatestProcessedDate(ArgumentMatchers.eq(outputTable), ArgumentMatchers.any[Option[LocalDate]]()))
            .thenReturn(Some(runDate.minusDays(9)))

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result.isEmpty)
        }
      }

      "rerun" when {
        val bk = mock(classOf[Bookkeeper])
        val infoDateExpression = "@runDate - 2"

        when(bk.getLatestDataChunk(outputTable, runDate.minusDays(7))).thenReturn(Some(null))
        when(bk.getLatestProcessedDate(outputTable, Some(runDate))).thenReturn(Some(runDate.minusDays(9)))

        "normal rerun" in {
          val params = ScheduleParams.Rerun(runDate.minusDays(5))

          val expected = Seq(pipeline.TaskPreDef(runDate.minusDays(7), TaskRunReason.Rerun))

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }

        "earlier than the minimum date" in {
          val params = ScheduleParams.Rerun(runDate.minusDays(365))

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result.isEmpty)
        }
      }

      "historical" when {
        val bk = mock(classOf[Bookkeeper])
        when(bk.getDataChunksCount(outputTable, Some(lastSaturday), Some(lastSaturday))).thenReturn(100)

        "fill gaps" in {
          val params = ScheduleParams.Historical(runDate.minusDays(14), nextSunday, inverseDateOrder = false, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(saturdayTwoWeeksAgo,
            nextSaturday)
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }

        "rerun all" in {
          val params = ScheduleParams.Historical(runDate.minusDays(14), nextSunday, inverseDateOrder = false, mode = RunMode.ForceRun)

          val expected = Seq(pipeline.TaskPreDef(saturdayTwoWeeksAgo, TaskRunReason.New),
            pipeline.TaskPreDef(lastSaturday, TaskRunReason.Rerun),
            pipeline.TaskPreDef(nextSaturday, TaskRunReason.New)
          )

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }

        "reverse order" in {
          val params = ScheduleParams.Historical(runDate.minusDays(14), nextSunday, inverseDateOrder = true, mode = RunMode.SkipAlreadyRan)

          val expected = Seq(nextSaturday,
            saturdayTwoWeeksAgo)
            .map(d => pipeline.TaskPreDef(d, TaskRunReason.New))

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }
      }
    }

    "monthly" when {
      val infoDateExpression = "beginOfMonth(@date)"
      val schedule = Schedule.Monthly(2 :: Nil)

      "normal execution" should {
        "default behavior with a monthly job" in {
          val minimumDate = LocalDate.parse("2022-05-30")
          val runDate = LocalDate.parse("2022-07-14")
          val params = ScheduleParams.Normal(runDate, 0, 0, 0, newOnly = false, lateOnly = false)

          val bk = mock(classOf[Bookkeeper])
          when(bk.getLatestProcessedDate(outputTable, Some(LocalDate.parse("2022-07-01"))))
            .thenReturn(Some(LocalDate.parse("2022-05-01")))

          val expected = Seq(
            pipeline.TaskPreDef(LocalDate.of(2022, 6, 1), TaskRunReason.Late),
            pipeline.TaskPreDef(LocalDate.of(2022, 7, 1), TaskRunReason.Late)
          )

          val result = strategyEvent.getDaysToRun(outputTable, dependencies, bk, infoDateExpression, schedule, params, initialSourcingDateExpr, minimumDate)

          assert(result == expected)
        }
      }
    }
  }
}
