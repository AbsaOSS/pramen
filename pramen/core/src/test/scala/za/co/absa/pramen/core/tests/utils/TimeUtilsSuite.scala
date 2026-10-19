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

  "getTimezoneFromTimestampString" should {
    "extract a positive offset from a timestamp with milliseconds" in {
      val result = TimeUtils.getTimezoneFromTimestampString("2026-10-16 15:02:28.436 +0200")

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHours(2))
      assert(result.get.toString == "+02:00")
    }

    "extract a positive offset from a timestamp without milliseconds" in {
      val result = TimeUtils.getTimezoneFromTimestampString("2026-10-16 15:02:28 +0200")

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHours(2))
      assert(result.get.toString == "+02:00")
    }

    "extract a negative offset with non-zero minutes" in {
      val result = TimeUtils.getTimezoneFromTimestampString("2026-10-16 15:02:28.436 -0530")

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHoursMinutes(-5, -30))
      assert(result.get.toString == "-05:30")
    }

    "extract a zero offset as UTC" in {
      val result = TimeUtils.getTimezoneFromTimestampString("2026-10-16 15:02:28.436 +0000")

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.UTC)
      assert(result.get.toString == "Z")
    }

    "ignore leading and trailing whitespace" in {
      val result = TimeUtils.getTimezoneFromTimestampString("   2026-10-16 15:02:28.436 +0200  ")

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHours(2))
    }

    "return the same offset regardless of the date and time part" in {
      val result1 = TimeUtils.getTimezoneFromTimestampString("2020-01-01 00:00:00 +0300")
      val result2 = TimeUtils.getTimezoneFromTimestampString("2025-12-31 23:59:59.999 +0300")

      assert(result1.isSuccess)
      assert(result2.isSuccess)
      assert(result1.get == result2.get)
      assert(result1.get == java.time.ZoneOffset.ofHours(3))
    }

    "return a failure for an empty string" in {
      val result = TimeUtils.getTimezoneFromTimestampString("")

      assert(result.isFailure)
    }

    "return a failure for a blank string" in {
      val result = TimeUtils.getTimezoneFromTimestampString("    ")

      assert(result.isFailure)
    }

    "return a failure when the offset is missing" in {
      val result = TimeUtils.getTimezoneFromTimestampString("2026-10-16 15:02:28.436")

      assert(result.isFailure)
    }

    "return a failure when only the date is provided" in {
      val result = TimeUtils.getTimezoneFromTimestampString("2026-10-16")

      assert(result.isFailure)
    }

    "return a failure for an invalid date" in {
      val result = TimeUtils.getTimezoneFromTimestampString("2026-13-45 15:02:28.436 +0200")

      assert(result.isFailure)
    }

    "return a failure for an out of range offset" in {
      val result = TimeUtils.getTimezoneFromTimestampString("2026-10-16 15:02:28.436 +2500")

      assert(result.isFailure)
    }

    "return a failure for a completely invalid string" in {
      val result = TimeUtils.getTimezoneFromTimestampString("not a timestamp")

      assert(result.isFailure)
    }
  }
  
  "getTimezoneFromTimestampStringAndEpochSeconds" should {
    // "2026-10-16 14:56:28" interpreted as UTC corresponds to 1792162588 epoch seconds
    val localAsUtcEpochSeconds = 1792162588L

    "extract a positive offset from a timestamp with milliseconds" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16 14:56:28.038", 1792155388L)

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHours(2))
      assert(result.get.toString == "+02:00")
    }

    "extract a positive offset from a timestamp without milliseconds" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16 14:56:28", 1792155388L)

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHours(2))
      assert(result.get.toString == "+02:00")
    }

    "extract a zero offset as UTC" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16 14:56:28.038", localAsUtcEpochSeconds)

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.UTC)
      assert(result.get.toString == "Z")
    }

    "extract a negative offset with non-zero minutes" in {
      val epochSeconds = localAsUtcEpochSeconds + 5 * 3600 + 30 * 60
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16 14:56:28.038", epochSeconds)

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHoursMinutes(-5, -30))
      assert(result.get.toString == "-05:30")
    }

    "extract a negative whole-hour offset" in {
      val epochSeconds = localAsUtcEpochSeconds + 10 * 3600
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16 14:56:28", epochSeconds)

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHours(-10))
      assert(result.get.toString == "-10:00")
    }

    "round the offset to the nearest minute when epoch seconds is slightly ahead" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16 14:56:28.038", 1792155388L + 20L)

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHours(2))
    }

    "round the offset to the nearest minute when epoch seconds is slightly behind" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16 14:56:28.038", 1792155388L - 25L)

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHours(2))
    }

    "round the offset down to the nearest 5 minutes" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16 14:56:28", 1792155388L - 40L)

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHoursMinutes(2, 0))
      assert(result.get.toString == "+02:00")
    }

    "round the offset up to the next 5 minutes when the difference is more than half of 5 minutes" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16 14:56:28", 1792155388L - 200L)

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHoursMinutes(2, 5))
      assert(result.get.toString == "+02:05")
    }

    "ignore leading and trailing whitespace" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("   2026-10-16 14:56:28.038  ", 1792155388L)

      assert(result.isSuccess)
      assert(result.get == java.time.ZoneOffset.ofHours(2))
    }

    "return a failure for an empty string" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("", 1792155388L)

      assert(result.isFailure)
    }

    "return a failure for a blank string" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("    ", 1792155388L)

      assert(result.isFailure)
    }

    "return a failure when only the date is provided" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16", 1792155388L)

      assert(result.isFailure)
    }

    "return a failure for an invalid date" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-13-45 14:56:28.038", 1792155388L)

      assert(result.isFailure)
    }

    "return a failure for a completely invalid string" in {
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("not a timestamp", 1792155388L)

      assert(result.isFailure)
    }

    "return a failure when the computed offset is out of range" in {
      val epochSeconds = localAsUtcEpochSeconds - 20 * 3600
      val result = TimeUtils.getTimezoneFromTimestampStringAndEpochSeconds("2026-10-16 14:56:28", epochSeconds)

      assert(result.isFailure)
    }
  }
}
