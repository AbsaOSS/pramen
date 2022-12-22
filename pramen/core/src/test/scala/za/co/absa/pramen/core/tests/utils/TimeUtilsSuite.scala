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

package za.co.absa.pramen.core.tests.utils

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.utils.TimeUtils

class TimeUtilsSuite extends AnyWordSpec {
  "prettyPrintElapsedTime" should {
    "return the number of milliseconds for values less than 1 second" in {
      assert(TimeUtils.prettyPrintElapsedTime(0) == "instantly")
      assert(TimeUtils.prettyPrintElapsedTime(10) == "10 ms")
      assert(TimeUtils.prettyPrintElapsedTime(100) == "100 ms")
      assert(TimeUtils.prettyPrintElapsedTime(999) == "999 ms")
    }

    "return the number of seconds for values less than 1 minute" in {
      assert(TimeUtils.prettyPrintElapsedTime(1000) == "1 second")
      assert(TimeUtils.prettyPrintElapsedTime(15000) == "15 seconds")
      assert(TimeUtils.prettyPrintElapsedTime(55500) == "55 seconds")
    }

    "return the number of minutes and seconds for values less than 1 hour" in {
      assert(TimeUtils.prettyPrintElapsedTime(61000) == "1 minute and 1 second")
      assert(TimeUtils.prettyPrintElapsedTime(65500) == "1 minute and 5 seconds")
      assert(TimeUtils.prettyPrintElapsedTime(305000) == "5 minutes and 5 seconds")
    }

    "return hours, minutes and seconds for values less than 1 day" in {
      assert(TimeUtils.prettyPrintElapsedTime(3600000) == "1 hour")
      assert(TimeUtils.prettyPrintElapsedTime(3660000) == "1 hour and 1 minute")
      assert(TimeUtils.prettyPrintElapsedTime(37500000) == "10 hours and 25 minutes")
      assert(TimeUtils.prettyPrintElapsedTime(37502500) == "10 hours and 25 minutes")
    }

    "return days, hours and minutes for large time intervals" in {
      assert(TimeUtils.prettyPrintElapsedTime(86400000) == "1 day")
      assert(TimeUtils.prettyPrintElapsedTime(90000000) == "1 day and 1 hour")
      assert(TimeUtils.prettyPrintElapsedTime(90060000) == "1 day, 1 hour and 1 minute")
      assert(TimeUtils.prettyPrintElapsedTime(123902500) == "1 day, 10 hours and 25 minutes")
      assert(TimeUtils.prettyPrintElapsedTime(1123142500) == "12 days, 23 hours and 59 minutes")
    }
  }

  "prettyPrintElapsedTimeShort" should {
    "return the number of milliseconds for values less than 1 second" in {
      assert(TimeUtils.prettyPrintElapsedTimeShort(0) == "instantly")
      assert(TimeUtils.prettyPrintElapsedTimeShort(10) == "instantly")
      assert(TimeUtils.prettyPrintElapsedTimeShort(100) == "instantly")
      assert(TimeUtils.prettyPrintElapsedTimeShort(999) == "instantly")
    }

    "return the number of seconds for values less than 1 minute" in {
      assert(TimeUtils.prettyPrintElapsedTimeShort(1000) == "00:00:01")
      assert(TimeUtils.prettyPrintElapsedTimeShort(15000) == "00:00:15")
      assert(TimeUtils.prettyPrintElapsedTimeShort(55500) == "00:00:55")
    }

    "return the number of minutes and seconds for values less than 1 hour" in {
      assert(TimeUtils.prettyPrintElapsedTimeShort(61000) == "00:01:01")
      assert(TimeUtils.prettyPrintElapsedTimeShort(65500) == "00:01:05")
      assert(TimeUtils.prettyPrintElapsedTimeShort(305000) == "00:05:05")
    }

    "return hours, minutes and seconds for values less than 1 day" in {
      assert(TimeUtils.prettyPrintElapsedTimeShort(3600000) == "01:00:00")
      assert(TimeUtils.prettyPrintElapsedTimeShort(3660000) == "01:01:00")
      assert(TimeUtils.prettyPrintElapsedTimeShort(37500000) == "10:25:00")
      assert(TimeUtils.prettyPrintElapsedTimeShort(37502500) == "10:25:02")
    }

    "return days, hours and minutes for large time intervals" in {
      assert(TimeUtils.prettyPrintElapsedTimeShort(86400000) == "1d, 00:00:00")
      assert(TimeUtils.prettyPrintElapsedTimeShort(90000000) == "1d, 01:00:00")
      assert(TimeUtils.prettyPrintElapsedTimeShort(90060000) == "1d, 01:01:00")
      assert(TimeUtils.prettyPrintElapsedTimeShort(123902500) == "1d, 10:25:02")
      assert(TimeUtils.prettyPrintElapsedTimeShort(1123142500) == "12d, 23:59:02")
    }

    "return an empty string when a negative value passed'" in {
      assert(TimeUtils.prettyPrintElapsedTimeShort(-10) == "")
    }
  }

  "withElapsedTimeMs" should {
    "measure elapsed time of a function call" in {
      val elapsedTimeMs = TimeUtils.withElapsedTimeMs {
        Thread.sleep(100)
      }

      assert(elapsedTimeMs > 0)
      assert(elapsedTimeMs < 2000)
    }
  }

  "withElapsedTimeStr" should {
    "measure elapsed time of a function call" in {
      val timeStr = TimeUtils.withElapsedTimeStr {
        Thread.sleep(100)
      }

      assert(timeStr.nonEmpty)
      assert(timeStr.contains("ms"))
    }
  }

  "withElapsedTimeLogged" should {
    "measure and log the elapsed time of a function call" in {
      TimeUtils.withElapsedTimeLogged("Elapsed time:") {
        Thread.sleep(100)
      }
    }
  }

}
