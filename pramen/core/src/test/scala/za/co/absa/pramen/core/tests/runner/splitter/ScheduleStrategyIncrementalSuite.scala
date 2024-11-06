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

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.jobdef.Schedule
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.core.bookkeeper.BookkeeperNull
import za.co.absa.pramen.core.runner.splitter.{RunMode, ScheduleParams, ScheduleStrategyIncremental}

import java.time.LocalDate

class ScheduleStrategyIncrementalSuite extends AnyWordSpec {
  private val minimumDate = LocalDate.of(2021, 1, 1)
  private val infoDate = LocalDate.of(2021, 2, 18)

  "normal run" when {
    "info date is defined" should {
      "run for the current info date when the job have never ran" in {
        val strategy = new ScheduleStrategyIncremental(None, true)
        val params = ScheduleParams.Normal(infoDate, 0, 0, newOnly = false, lateOnly = false)

        val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

        assert(dates.length == 1)
        assert(dates.head.infoDate == infoDate)
        assert(dates.head.reason == TaskRunReason.New)
      }

      "run for the current info date when the job ran today before" in {
        val strategy = new ScheduleStrategyIncremental(Some(infoDate), true)
        val params = ScheduleParams.Normal(infoDate, 0, 0, newOnly = false, lateOnly = false)

        val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

        assert(dates.length == 1)
        assert(dates.head.infoDate == infoDate)
        assert(dates.head.reason == TaskRunReason.New)
      }

      "run for the current info date when the job ran several before track days = 2" in {
        val strategy = new ScheduleStrategyIncremental(Some(infoDate.minusDays(10)), true)
        val params = ScheduleParams.Normal(infoDate, 2, 0, newOnly = false, lateOnly = false)

        val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

        assert(dates.length == 3)
        assert(dates.head.infoDate == infoDate.minusDays(2))
        assert(dates.head.reason == TaskRunReason.Late)
        assert(dates(1).infoDate == infoDate.minusDays(1))
        assert(dates(1).reason == TaskRunReason.New)
        assert(dates(2).infoDate == infoDate)
        assert(dates(2).reason == TaskRunReason.New)
      }

      "run for the current info date when the job ran several before track days = -1" in {
        val strategy = new ScheduleStrategyIncremental(Some(infoDate.minusDays(3)), true)
        val params = ScheduleParams.Normal(infoDate, -1, 0, newOnly = false, lateOnly = false)

        val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

        assert(dates.length == 4)
        assert(dates.head.infoDate == infoDate.minusDays(3))
        assert(dates.head.reason == TaskRunReason.Late)
        assert(dates(1).infoDate == infoDate.minusDays(2))
        assert(dates(1).reason == TaskRunReason.Late)
        assert(dates(2).infoDate == infoDate.minusDays(1))
        assert(dates(2).reason == TaskRunReason.New)
        assert(dates(3).infoDate == infoDate)
        assert(dates(3).reason == TaskRunReason.New)
      }

      "run for the current info date new only" in {
        val strategy = new ScheduleStrategyIncremental(Some(infoDate.minusDays(3)), true)
        val params = ScheduleParams.Normal(infoDate, -1, 0, newOnly = true, lateOnly = false)

        val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

        assert(dates.length == 2)
        assert(dates.head.infoDate == infoDate.minusDays(1))
        assert(dates.head.reason == TaskRunReason.New)
        assert(dates(1).infoDate == infoDate)
        assert(dates(1).reason == TaskRunReason.New)
      }

      "run for the current info date late only" in {
        val strategy = new ScheduleStrategyIncremental(Some(infoDate.minusDays(3)), true)
        val params = ScheduleParams.Normal(infoDate, -1, 0, newOnly = false, lateOnly = true)

        val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

        assert(dates.length == 2)
        assert(dates.head.infoDate == infoDate.minusDays(3))
        assert(dates.head.reason == TaskRunReason.Late)
        assert(dates(1).infoDate == infoDate.minusDays(2))
        assert(dates(1).reason == TaskRunReason.Late)
      }
    }

    "info date is not defined" should {
      "run for the current info date when the job have never ran" in {
        val strategy = new ScheduleStrategyIncremental(None, false)
        val params = ScheduleParams.Normal(infoDate, 0, 0, newOnly = false, lateOnly = false)

        val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

        assert(dates.length == 1)
        assert(dates.head.infoDate == infoDate)
        assert(dates.head.reason == TaskRunReason.New)
      }

      "run for the current info date when the job ran today before" in {
        val strategy = new ScheduleStrategyIncremental(Some(infoDate), false)
        val params = ScheduleParams.Normal(infoDate, 0, 0, newOnly = false, lateOnly = false)

        val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

        assert(dates.length == 1)
        assert(dates.head.infoDate == infoDate)
        assert(dates.head.reason == TaskRunReason.New)
      }

      "run for the current info date when the job ran some time ago" in {
        val strategy = new ScheduleStrategyIncremental(Some(infoDate.minusDays(5)), false)
        val params = ScheduleParams.Normal(infoDate, 0, 0, newOnly = false, lateOnly = false)

        val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

        assert(dates.length == 1)
        assert(dates.head.infoDate == infoDate)
        assert(dates.head.reason == TaskRunReason.New)
      }
    }
  }

  "re-run" when {
    "info date is defined" in {
      val strategy = new ScheduleStrategyIncremental(Some(infoDate.plusDays(5)), true)
      val params = ScheduleParams.Rerun(infoDate)

      val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

      assert(dates.length == 1)
      assert(dates.head.infoDate == infoDate)
      assert(dates.head.reason == TaskRunReason.Rerun)
    }

    "info date is not defined" in {
      val strategy = new ScheduleStrategyIncremental(Some(infoDate.plusDays(5)), false)
      val params = ScheduleParams.Rerun(infoDate)

      val dates = strategy.getDaysToRun("table1", Seq.empty, null, "@runDate", Schedule.Incremental, params, null, minimumDate)

      assert(dates.length == 1)
      assert(dates.head.infoDate == infoDate)
      assert(dates.head.reason == TaskRunReason.Rerun)
    }
  }

  "historical run" when {
    val bk = new BookkeeperNull

    "info date is defined" in {
      val strategy = new ScheduleStrategyIncremental(Some(infoDate.plusDays(5)), true)
      val params = ScheduleParams.Historical(infoDate.minusDays(2), infoDate, inverseDateOrder = false, RunMode.ForceRun)

      val dates = strategy.getDaysToRun("table1", Seq.empty, bk, "@runDate", Schedule.Incremental, params, null, minimumDate)

      dates.foreach(println)

      assert(dates.length == 3)
      assert(dates.head.infoDate == infoDate.minusDays(2))
      assert(dates.head.reason == TaskRunReason.New)
      assert(dates(1).infoDate == infoDate.minusDays(1))
      assert(dates(1).reason == TaskRunReason.New)
      assert(dates(2).infoDate == infoDate)
      assert(dates(2).reason == TaskRunReason.New)
    }

    "info date is not defined" in {
      val strategy = new ScheduleStrategyIncremental(Some(infoDate.plusDays(5)), false)
      val params = ScheduleParams.Historical(infoDate.minusDays(2), infoDate, inverseDateOrder = true, RunMode.ForceRun)

      val dates = strategy.getDaysToRun("table1", Seq.empty, bk, "@runDate", Schedule.Incremental, params, null, minimumDate)

      assert(dates.length == 3)
      assert(dates.head.infoDate == infoDate)
      assert(dates.head.reason == TaskRunReason.New)
      assert(dates(1).infoDate == infoDate.minusDays(1))
      assert(dates(1).reason == TaskRunReason.New)
      assert(dates(2).infoDate == infoDate.minusDays(2))
      assert(dates(2).reason == TaskRunReason.New)
    }
  }
}
