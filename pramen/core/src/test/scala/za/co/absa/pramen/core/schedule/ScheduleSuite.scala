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

package za.co.absa.pramen.core.schedule

import com.typesafe.config.ConfigException.WrongType
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.schedule.Schedule._

import java.time.{DayOfWeek, LocalDate}

class ScheduleSuite extends AnyWordSpec {

  "Schedule.fromConfig" should {
    "Deserialize daily jobs" when {
      "a normal daily job is provided" in {
        val config = ConfigFactory.parseString(s"$SCHEDULE_TYPE_KEY = daily")
        val schedule = fromConfig(config)

        assert(schedule == Schedule.EveryDay())
      }
    }

    "Deserialize weekly jobs" when {
      "a single day weekly job is provided" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = weekly
             |$SCHEDULE_DAYS_OF_WEEK_KEY = [ 7 ]""".stripMargin)
        val schedule = fromConfig(config)

        assert(schedule == Schedule.Weekly(DayOfWeek.SUNDAY :: Nil))
      }
      "a multiple days weekly job is provided" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = weekly
             |$SCHEDULE_DAYS_OF_WEEK_KEY = [ 1, 7 ]""".stripMargin)
        val schedule = fromConfig(config)

        assert(schedule == Schedule.Weekly(DayOfWeek.MONDAY :: DayOfWeek.SUNDAY :: Nil))
      }

      "throw an exception when a wrong week day is provided 1" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = weekly
             |$SCHEDULE_DAYS_OF_WEEK_KEY = [ 1, 7, 8 ]""".stripMargin)
        intercept[java.time.DateTimeException] {
          fromConfig(config)
        }
      }

      "throw an exception when a wrong week day is provided 2" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = weekly
             |$SCHEDULE_DAYS_OF_WEEK_KEY = [ 0 ]""".stripMargin)
        intercept[java.time.DateTimeException] {
          fromConfig(config)
        }
      }

      "throw an exception when no week days are provided" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = weekly
             |$SCHEDULE_DAYS_OF_WEEK_KEY = [ ]""".stripMargin)
        val ex = intercept[IllegalArgumentException] {
          fromConfig(config)
        }

        assert(ex.getMessage.contains("No days of week are provided"))
      }

      "throw an exception when the type is not an array of ints" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = weekly
             |$SCHEDULE_DAYS_OF_WEEK_KEY = aaa""".stripMargin)
        val ex = intercept[WrongType] {
          fromConfig(config)
        }

        assert(ex.getMessage.contains("has type STRING rather than LIST"))
      }
    }

    "Deserialize monthly jobs" when {
      "a single day monthly job is provided" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = monthly
             |$SCHEDULE_DAYS_OF_MONTH_KEY = [ 1 ]""".stripMargin)
        val schedule = fromConfig(config)

        assert(schedule == Monthly(1 :: Nil))
      }
      "a multiple days weekly job is provided" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = monthly
             |$SCHEDULE_DAYS_OF_MONTH_KEY = [ 1, 2, 31 ]""".stripMargin)
        val schedule = fromConfig(config)

        assert(schedule == Monthly(1 :: 2 :: 31 :: Nil))
      }

      "throw an exception when a wrong month day is provided 1" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = monthly
             |$SCHEDULE_DAYS_OF_MONTH_KEY = [ 1, 7, 32 ]""".stripMargin)
        val ex = intercept[IllegalArgumentException] {
          fromConfig(config)
        }

        assert(ex.getMessage.contains("Invalid day of month"))
      }

      "throw an exception when a wrong month day is provided 2" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = monthly
             |$SCHEDULE_DAYS_OF_MONTH_KEY = [ 0 ]""".stripMargin)
        val ex = intercept[IllegalArgumentException] {
          fromConfig(config)
        }

        assert(ex.getMessage.contains("Invalid day of month"))
      }

      "throw an exception when no month days are provided" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = monthly
             |$SCHEDULE_DAYS_OF_MONTH_KEY = [ ]""".stripMargin)
        val ex = intercept[IllegalArgumentException] {
          fromConfig(config)
        }

        assert(ex.getMessage.contains("No days of month are provided"))
      }

      "throw an exception when the type is not an array of ints" in {
        val config = ConfigFactory.parseString(
          s"""$SCHEDULE_TYPE_KEY = monthly
             |$SCHEDULE_DAYS_OF_MONTH_KEY = aaa""".stripMargin)
        val ex = intercept[WrongType] {
          fromConfig(config)
        }

        assert(ex.getMessage.contains("has type STRING rather than LIST"))
      }

    }

    "throw an exception on a wrong schedule type" in {
      val config = ConfigFactory.parseString(s"$SCHEDULE_TYPE_KEY = dummy")

      val ex = intercept[IllegalArgumentException] {
        fromConfig(config)
      }

      assert(ex.getMessage == "Unknown schedule type: dummy")
    }
  }

  "Schedule.isEnabled" should {
    val monday = LocalDate.of(2020, 8, 10)
    val tuesday = LocalDate.of(2020, 8, 11)
    val wednesday = LocalDate.of(2020, 8, 12)
    val thursday = LocalDate.of(2020, 8, 13)
    val friday = LocalDate.of(2020, 8, 14)
    val saturday = LocalDate.of(2020, 8, 15)
    val sunday = LocalDate.of(2020, 8, 16)

    "Always return true for everyday jobs" in {
      val schedule = EveryDay()

      assert(schedule.isEnabled(monday))
      assert(schedule.isEnabled(tuesday))
      assert(schedule.isEnabled(wednesday))
      assert(schedule.isEnabled(thursday))
      assert(schedule.isEnabled(friday))
      assert(schedule.isEnabled(saturday))
      assert(schedule.isEnabled(sunday))
    }

    "Return true for specific week days for weekly jobs" in {
      val schedule = Weekly(DayOfWeek.MONDAY :: DayOfWeek.SUNDAY :: Nil)

      assert(schedule.isEnabled(monday))
      assert(!schedule.isEnabled(tuesday))
      assert(!schedule.isEnabled(wednesday))
      assert(!schedule.isEnabled(thursday))
      assert(!schedule.isEnabled(friday))
      assert(!schedule.isEnabled(saturday))
      assert(schedule.isEnabled(sunday))
    }

    "Return true for specific days of month for monthly jobs" in {
      val schedule = Monthly(1 :: 10 :: 15 :: Nil)

      assert(schedule.isEnabled(LocalDate.of(2020, 8, 1)))
      assert(schedule.isEnabled(LocalDate.of(2020, 9, 1)))

      assert(schedule.isEnabled(monday))
      assert(!schedule.isEnabled(tuesday))
      assert(!schedule.isEnabled(wednesday))
      assert(!schedule.isEnabled(thursday))
      assert(!schedule.isEnabled(friday))
      assert(schedule.isEnabled(saturday))
      assert(!schedule.isEnabled(sunday))
    }

  }

}
