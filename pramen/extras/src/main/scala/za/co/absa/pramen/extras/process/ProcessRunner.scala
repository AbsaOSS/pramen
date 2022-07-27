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

package za.co.absa.pramen.builtin.process

import java.io.{BufferedReader, InputStreamReader}

import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.utils.{CircularBuffer, StringUtils}

import scala.util.control.NonFatal
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class ProcessRunner(cmd: String,
                    recordCountFilterRegEx: String,
                    zeroRecordSuccessRegEx: Option[String],
                    noDataFailureRegEx: Option[String],
                    outputFiltersRegEx: Seq[String],
                    includeLogLines: Int) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private var recordCountOpt: Option[Long] = None

  private var failedNoData: Boolean = false

  private var failedLine: String = ""

  private val lastLoggedRecords = if (includeLogLines > 0) Some (new CircularBuffer[String](includeLogLines)) else None

  def getCmdLog: Array[String] = lastLoggedRecords.map(_.get()).getOrElse(Array.empty[String])

  /**
    * Runs the command.
    *
    * @return The exit status of the program.
    */
  def run(): Int = {
    val processBuilder = new ProcessBuilder(StringUtils.tokenize(cmd): _*)
      .redirectErrorStream(true)

    log.info(s"Starting '$cmd'")
    val process = processBuilder.start

    val read = new BufferedReader(new InputStreamReader(
      process.getInputStream))

    recordCountOpt = getRecordCountFromOutput(read, recordCountFilterRegEx)

    read.close()

    val exitStatus = process.waitFor()

    log.info(s"The command completed with exit status $exitStatus.")
    recordCountOpt.foreach(num => log.info(s"The number of records written: $num."))

    read.close()

    exitStatus
  }

  def getRecordCount: Option[Long] = recordCountOpt

  def isFailedNoData: Boolean = failedNoData

  def getFailedLine: String = failedLine

  private[pramen] def getRecordCountFromOutput(read: BufferedReader, outputRecordRegEx: String): Option[Long] = {
    val cmdOutputLogger = LoggerFactory.getLogger("cmd")
    val pattern = s"$outputRecordRegEx".r

    outputFiltersRegEx.foreach(s => cmdOutputLogger.debug(s"Output filter: $s"))

    val zeroRecordRegExOpt = zeroRecordSuccessRegEx.map(_.r)
    val noDataFailureRegExOpt = noDataFailureRegEx.map(_.r)
    val filters = outputFiltersRegEx.map(_.r)

    var recordCountOptToReturn: Option[Long] = None

    var line: String = read.readLine
    while (line != null) {
      if (line.nonEmpty && !filters.exists(_.findFirstIn(line).nonEmpty)) {
        if (!censorLine(line)) {
          lastLoggedRecords.foreach(_.add(line))
        }
        cmdOutputLogger.info(line)
      }

      val recordCountOpt = Try {
        getFirstGroupTokenMatchLong(pattern, line)
      } match {
        case Success(numOpt) =>
          numOpt
        case Failure(ex) =>
          log.error(s"Error parsing the number of output records", ex)
          None
      }

      val isZeroRecordSuccess = zeroRecordRegExOpt.exists(r => matchesPattern(r, line))
      val isNoDataFailure = noDataFailureRegExOpt.exists(r => matchesPattern(r, line))

      recordCountOpt.foreach(num => {
        failedNoData = false
        recordCountOptToReturn = recordCountOpt
        log.info(s"Parsed number of records: $num")
      })

      if (isZeroRecordSuccess) {
        failedNoData = false
        log.info(s"Parsed zero record success.")
        recordCountOptToReturn = Some(0)
      }

      if (isNoDataFailure) {
        failedNoData = true
        failedLine = line
        log.warn(s"Parsed no data failure: $line")
      }

      line = read.readLine
    }
    recordCountOptToReturn
  }

  private[pramen] def getFirstGroupTokenMatchLong(pattern: Regex, s: String): Option[Long] = {
    val matched = pattern.findAllIn(s).matchData

    if (matched.hasNext) {
      val firstMatch = matched.next
      if (firstMatch.groupCount < 1) {
        throw new IllegalArgumentException(s"An output string matches the pattern, but cannot extract a group.\n" +
          s"String: $s\nPattern: $recordCountFilterRegEx\nMatch: $firstMatch")
      } else {
        val str = firstMatch.group(1)
        val numOpt = try {
          Option(str.trim.toLong)
        } catch {
          case NonFatal(ex) =>
            throw new IllegalArgumentException(s"Unable to parse '$str' as long.\n" +
              s"String: $s\nPattern: $recordCountFilterRegEx\n", ex)
        }
        numOpt
      }
    } else {
      None
    }
  }

  private[pramen] def matchesPattern(pattern: Regex, s: String): Boolean = {
    pattern.findFirstIn(s).isDefined
  }

  private def censorLine(inputString: String): Boolean = {
    if (inputString.toLowerCase().contains("password")) {
      true
    } else {
      false
    }
  }

}
