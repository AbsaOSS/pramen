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

package za.co.absa.pramen.core.utils

import org.slf4j.LoggerFactory

import java.time.{Duration, Instant}
import java.util.Locale

object TimeUtils {
  import scala.math.Integral.Implicits._

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Returns a pretty time description between 2 instances.
    *
    * @param start Start instant.
    * @param finish End instant.
    * @return Elapsed time between start and finish in a human-readable format.
    */
  def getElapsedTimeStr(start: Instant, finish: Instant): String = {
    val duration = Duration.between(start, finish).toMillis
    TimeUtils.prettyPrintElapsedTime(duration)
  }

  /**
    * Taken from https://github.com/AbsaOSS/hermes/blob/develop/utils/src/main/scala/za/co/absa/hermes/utils/HelperFunctions.scala
    *
    * Pretty prints elapsed time. If given 91441000 will return "1 day, 1 hour, 24 minutes"
    *
    * @param elapsedTimeMs Elapsed time in milliseconds you want to pretty print
    * @return Returns a string format with human readable time segments
    */
  def prettyPrintElapsedTime(elapsedTimeMs: Long): String = {

    def stringify(count: Long, noun: String, factor: Long = 1): Option[String] = {
      val formatted = (count, factor) match {
        case (0, _) => None
        case (c, 1) => Option("%d".formatLocal(Locale.US, c))
        case (c, f) => Option("%d".formatLocal(Locale.US, c / f))
      }
      formatted.map(s => s"$s $noun${
        if (count == factor) {
          ""
        } else {
          "s"
        }
      }")
    }

    val (numberOfDays, remainingAfterDay) = elapsedTimeMs /% millisecondsPerDay
    val (numberOfHours, remainingAfterHours) = remainingAfterDay /% millisecondsPerHour
    val (numberOfMinutes, numberOfMilliseconds) = remainingAfterHours /% millisecondsPerMinute

    val daysString = stringify(numberOfDays, "day")
    val hoursString = stringify(numberOfHours, "hour")
    val minutesString = stringify(numberOfMinutes, "minute")
    val secondsString = if (numberOfHours > 0 || numberOfDays > 0) {
      None
    } else {
      if (elapsedTimeMs < 1000L) {
        if (elapsedTimeMs == 0L) {
          Some("instantly")
        } else {
          Some(s"$elapsedTimeMs ms")
        }
      } else {
        stringify(numberOfMilliseconds, "second", millisecondsPerSecond)
      }
    }

    val nonZeroSegments = Array(daysString, hoursString, minutesString, secondsString).flatten

    nonZeroSegments.length match {
      case 0 => "0 seconds"
      case 1 => nonZeroSegments(0)
      case len =>
        val lastSegmentIndex = len - 1
        nonZeroSegments.slice(0, lastSegmentIndex).mkString(", ") + " and " + nonZeroSegments(lastSegmentIndex)
    }
  }

  /**
    *
    * Pretty prints elapsed time (short version). If given 91441000 will return "1d, 01:24:00"
    *
    * @param elapsedTimeMs Elapsed time in milliseconds you want to pretty print
    * @return Returns a string format with human readable time segments
    */
  def prettyPrintElapsedTimeShort(elapsedTimeMs: Long): String = {
    val (numberOfDays, remainingAfterDay) = elapsedTimeMs /% millisecondsPerDay
    val (numberOfHours, remainingAfterHours) = remainingAfterDay /% millisecondsPerHour
    val (numberOfMinutes, numberOfMilliseconds) = remainingAfterHours /% millisecondsPerMinute
    val numberOfSeconds = numberOfMilliseconds / millisecondsPerSecond

    if (elapsedTimeMs < 0) {
      ""
    } else if (numberOfDays > 0) {
      f"$numberOfDays%dd, $numberOfHours%02d:$numberOfMinutes%02d:$numberOfSeconds%02d"
    } else if (elapsedTimeMs < millisecondsPerSecond) {
      "instantly"
    } else {
      f"$numberOfHours%02d:$numberOfMinutes%02d:$numberOfSeconds%02d"
    }
  }

  def withElapsedTimeMs(f: => Unit): Long = {
    val start = Instant.now()
    f
    val finish = Instant.now()

    Duration.between(start, finish).toMillis
  }

  def withElapsedTimeStr(f: => Unit): String = {
    val start = Instant.now()
    f
    val finish = Instant.now()

    getElapsedTimeStr(start, finish)
  }

  def withElapsedTimeLogged(description: String)(f: => Unit): Unit = {
    val elapsedTimeStr = withElapsedTimeStr(f)

    log.info(s"$description $elapsedTimeStr")
  }

  private val millisecondsPerSecond: Long = 1000
  private val millisecondsPerMinute: Long = millisecondsPerSecond * 60
  private val millisecondsPerHour: Long = millisecondsPerMinute * 60
  private val millisecondsPerDay: Long = millisecondsPerHour * 24
}
