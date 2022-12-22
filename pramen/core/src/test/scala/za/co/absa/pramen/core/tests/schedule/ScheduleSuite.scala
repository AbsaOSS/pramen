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

package za.co.absa.pramen.core.tests.schedule

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.schedule.Schedule

import java.time.{DayOfWeek, LocalDate}

class ScheduleSuite extends AnyWordSpec {
  private val beginOfMonth = LocalDate.of(2022, 1, 1)
  private val secondDayOfMonth = LocalDate.of(2022, 1, 2)
  private val secondToEndOfMonth = LocalDate.of(2022, 1, 30)
  private val endOfMonth = LocalDate.of(2022, 1, 31)

  private val monday = LocalDate.of(2022, 1, 3)
  private val tuesday = LocalDate.of(2022, 1, 4)
  private val wednesday = LocalDate.of(2022, 1, 5)
  private val thursday = LocalDate.of(2022, 1, 6)
  private val friday = LocalDate.of(2022, 1, 7)
  private val saturday = LocalDate.of(2022, 1, 8)
  private val sunday = LocalDate.of(2022, 1, 9)

  "isEnabled()" should {
    "work for daily schedules" in {
      val schedule = Schedule.EveryDay()

      assert(schedule.isEnabled(beginOfMonth))
      assert(schedule.isEnabled(secondDayOfMonth))
      assert(schedule.isEnabled(secondToEndOfMonth))
      assert(schedule.isEnabled(endOfMonth))
      assert(schedule.isEnabled(saturday))
      assert(schedule.isEnabled(sunday))
      assert(schedule.isEnabled(monday))
    }

    "work for weekly schedules" when {
      "weekly" in {
        val schedule = Schedule.Weekly(Seq(saturday.getDayOfWeek))

        assert(schedule.isEnabled(saturday))
        assert(!schedule.isEnabled(sunday))
        assert(!schedule.isEnabled(monday))
        assert(!schedule.isEnabled(tuesday))
        assert(!schedule.isEnabled(wednesday))
        assert(!schedule.isEnabled(thursday))
        assert(!schedule.isEnabled(friday))
      }

      "by-weekly" in {
        val schedule = Schedule.Weekly(Seq(sunday.getDayOfWeek, monday.getDayOfWeek))

        assert(!schedule.isEnabled(saturday))
        assert(schedule.isEnabled(sunday))
        assert(schedule.isEnabled(monday))
        assert(!schedule.isEnabled(tuesday))
        assert(!schedule.isEnabled(wednesday))
        assert(!schedule.isEnabled(thursday))
        assert(!schedule.isEnabled(friday))
      }
    }

    "work for monthly schedules" when {
      "normal month days" in {
        val schedule = Schedule.Monthly(Seq(2, 30))

        assert(!schedule.isEnabled(beginOfMonth))
        assert(schedule.isEnabled(secondDayOfMonth))
        assert(schedule.isEnabled(secondToEndOfMonth))
        assert(!schedule.isEnabled(endOfMonth))
      }

      "negative month days 1" in {
        val schedule = Schedule.Monthly(Seq(-1))

        assert(!schedule.isEnabled(beginOfMonth))
        assert(!schedule.isEnabled(secondDayOfMonth))
        assert(!schedule.isEnabled(secondToEndOfMonth))
        assert(schedule.isEnabled(endOfMonth))
      }

      "negative month days 2" in {
        val schedule = Schedule.Monthly(Seq(-1, -2))

        assert(!schedule.isEnabled(beginOfMonth))
        assert(!schedule.isEnabled(secondDayOfMonth))
        assert(schedule.isEnabled(secondToEndOfMonth))
        assert(schedule.isEnabled(endOfMonth))
      }

      "mixed month days" in {
        val schedule = Schedule.Monthly(Seq(1, -2))

        assert(schedule.isEnabled(beginOfMonth))
        assert(!schedule.isEnabled(secondDayOfMonth))
        assert(schedule.isEnabled(secondToEndOfMonth))
        assert(!schedule.isEnabled(endOfMonth))
      }
    }
  }

  "fromConfig()" should {
    "work for daily schedules" in {
      val conf = getConfigString("daily")
      val schedule = Schedule.fromConfig(conf)

      assert(schedule == Schedule.EveryDay())
    }

    "work for weekly schedules" in {
      val conf = getConfigString("weekly", "6, 7")
      val schedule = Schedule.fromConfig(conf)

      assert(schedule == Schedule.Weekly(Seq(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY)))
    }

    "work for monthly schedules" when {
      "days of month are numeric" in {
        val conf = getConfigString("monthly", "2, -1")
        val schedule = Schedule.fromConfig(conf)

        assert(schedule == Schedule.Monthly(Seq(2, -1)))
      }

      "days of month contains 'last'" in {
        val conf = getConfigString("monthly", """5, "last"""")
        val schedule = Schedule.fromConfig(conf)

        assert(schedule == Schedule.Monthly(Seq(5, -1)))
      }

      "days of month contains 'L'" in {
        val conf = getConfigString("monthly", "L")
        val schedule = Schedule.fromConfig(conf)

        assert(schedule == Schedule.Monthly(Seq(-1)))
      }

      "throw an exception when days of month is empty" in {
        val conf = getConfigString("monthly", "")

        assertThrows[IllegalArgumentException] {
          Schedule.fromConfig(conf)
        }
      }

      "throw an exception when days of month are not parsable" in {
        val conf = getConfigString("monthly", "first")

        assertThrows[IllegalArgumentException] {
          Schedule.fromConfig(conf)
        }
      }

      "throw an exception when days of month are out of range" in {
        val conf = getConfigString("monthly", "32")

        assertThrows[IllegalArgumentException] {
          Schedule.fromConfig(conf)
        }
      }
    }
  }

  private def getConfigString(scheduleType: String, daysList: String= ""): Config = {
    val daysString = scheduleType match {
      case "weekly" => s"""days.of.week = [ $daysList ]"""
      case "monthly" => s"""days.of.month = [ $daysList ]"""
      case _ => ""
    }
    ConfigFactory.parseString(
      s"""
         |schedule {
         |  type = "$scheduleType"
         |  $daysString
         |}
         |""".stripMargin)
  }

}
