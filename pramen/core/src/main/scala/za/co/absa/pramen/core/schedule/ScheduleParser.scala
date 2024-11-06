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

import com.typesafe.config.Config
import za.co.absa.pramen.api.jobdef.Schedule

import java.time.{DayOfWeek, LocalDate}
import scala.collection.JavaConverters._
import scala.util.Try

object ScheduleParser {
  val SCHEDULE_TYPE_KEY = "schedule.type"
  val SCHEDULE_DAYS_OF_WEEK_KEY = "schedule.days.of.week"
  val SCHEDULE_DAYS_OF_MONTH_KEY = "schedule.days.of.month"

  def fromConfig(conf: Config): Schedule = {
    conf.getString(SCHEDULE_TYPE_KEY) match {
      case "incremental"   => Schedule.Incremental
      case "daily"         => Schedule.EveryDay()
      case "weekly"        => Schedule.Weekly(getDaysOfWeek(conf))
      case "monthly"       => Schedule.Monthly(getDaysOfMonth(conf))
      case s               => throw new IllegalArgumentException(s"Unknown schedule type: $s")
    }
  }

  private def getDaysOfWeek(conf: Config): Seq[DayOfWeek] = {
    val weekDayNums = conf.getIntList(SCHEDULE_DAYS_OF_WEEK_KEY).asScala

    if (weekDayNums.isEmpty) {
      throw new IllegalArgumentException(s"No days of week are provided $SCHEDULE_DAYS_OF_WEEK_KEY")
    }

    weekDayNums.map(num => DayOfWeek.of(num)).toSeq
  }

  private def getDaysOfMonth(conf: Config): Seq[Int] = {
    val monthDayNums = conf.getStringList(SCHEDULE_DAYS_OF_MONTH_KEY).asScala.map { str =>
      val strUpper = str.trim.toUpperCase
      val num = if (strUpper == "LAST" || strUpper == "L") {
        -1
      } else {
        Try(str.toInt).getOrElse(throw new IllegalArgumentException(s"Invalid day of month: $str"))
      }
      num
    }

    if (monthDayNums.isEmpty) {
      throw new IllegalArgumentException(s"No days of month are provided $SCHEDULE_DAYS_OF_WEEK_KEY")
    }

    monthDayNums.foreach(day => {
      if (day < -31 || day > 31 || day == 0) {
        throw new IllegalArgumentException(s"Invalid day of month: $day")
      }
    })
    monthDayNums.toSeq
  }
}
