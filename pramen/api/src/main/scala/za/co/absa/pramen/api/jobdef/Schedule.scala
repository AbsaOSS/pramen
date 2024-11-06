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

package za.co.absa.pramen.api.jobdef

import java.time.{DayOfWeek, LocalDate}

sealed trait Schedule {
  def isEnabled(day: LocalDate): Boolean
}

object Schedule {
  case object Incremental extends Schedule {
    def isEnabled(day: LocalDate): Boolean = true

    override def toString: String = "incremental"
  }

  case class EveryDay() extends Schedule {
    def isEnabled(day: LocalDate): Boolean = true

    override def toString: String = "daily"
  }

  case class Weekly(days: Seq[DayOfWeek]) extends Schedule {
    def isEnabled(day: LocalDate): Boolean = days.contains(day.getDayOfWeek)

    override def toString: String = s"weekly (${days.mkString(", ")})"
  }

  case class Monthly(days: Seq[Int]) extends Schedule {
    val hasPositiveDays: Boolean = days.exists(_ > 0)
    val hasNegativeDays: Boolean = days.exists(_ < 0)

    def isEnabled(day: LocalDate): Boolean = {
      val isInPositives = hasPositiveDays && days.contains(day.getDayOfMonth)
      val isInNegatives = hasNegativeDays && days.contains(-day.lengthOfMonth() + day.getDayOfMonth - 1)
      isInPositives || isInNegatives
    }

    override def toString: String = s"monthly (${days.mkString(", ")})"
  }
}
