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

package za.co.absa.pramen.framework.schedule

import com.typesafe.config.Config

import java.time.{DayOfWeek, LocalDate}
import scala.collection.JavaConverters._

sealed trait Schedule {
  def isEnabled(day: LocalDate): Boolean
}

object Schedule {
  val SCHEDULE_TYPE_KEY = "schedule.type"
  val SCHEDULE_DAYS_OF_WEEK_KEY = "schedule.days.of.week"
  val SCHEDULE_DAYS_OF_MONTH_KEY = "schedule.days.of.month"

  case class EveryDay() extends Schedule {
    def isEnabled(day: LocalDate): Boolean = true
  }

  case class Weekly(days: Seq[DayOfWeek]) extends Schedule {
    def isEnabled(day: LocalDate): Boolean = days.contains(day.getDayOfWeek)
  }

  case class Monthly(days: Seq[Int]) extends Schedule {
    def isEnabled(day: LocalDate): Boolean = days.contains(day.getDayOfMonth)
  }

  def fromConfig(conf: Config): Schedule = {
    conf.getString(SCHEDULE_TYPE_KEY) match {
      case "daily" => EveryDay()
      case "weekly" => Weekly(getDaysOfWeek(conf))
      case "monthly" => Monthly(getDaysOfMonth(conf))
      case s => throw new IllegalArgumentException(s"Unknown schedule type: $s")
    }
  }

  private def getDaysOfWeek(conf: Config): Seq[DayOfWeek] = {
    val weekDayNums = conf.getIntList(SCHEDULE_DAYS_OF_WEEK_KEY).asScala

    if (weekDayNums.isEmpty) {
      throw new IllegalArgumentException(s"No days of week are provided $SCHEDULE_DAYS_OF_WEEK_KEY")
    }

    weekDayNums.map(num => DayOfWeek.of(num))
  }

  private def getDaysOfMonth(conf: Config): Seq[Int] = {
    val monthDayNums = conf.getIntList(SCHEDULE_DAYS_OF_MONTH_KEY).asScala.map(_.toInt)

    if (monthDayNums.isEmpty) {
      throw new IllegalArgumentException(s"No days of month are provided $SCHEDULE_DAYS_OF_WEEK_KEY")
    }

    monthDayNums.foreach(day => {
      if (day < 1 || day > 31) {
        throw new IllegalArgumentException(s"Invalid day of month: $day")
      }
    })
    monthDayNums
  }
}