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

package za.co.absa.pramen.core.process

import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.pramen.core.utils.{CircularBuffer, StringUtils}

import java.io.{BufferedReader, InputStreamReader}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.util.matching.Regex

class ProcessRunnerImpl(val includeOutputLines: Int,
                        val logStdOut: Boolean,
                        val logStdErr: Boolean,
                        val stdOutLogPrefix: String,
                        val stdErrLogPrefix: String,
                        val redirectErrorStream: Boolean,
                        val recordCountRegEx: Option[String],
                        val zeroRecordsSuccessRegEx: Option[String],
                        val failureRegEx: Option[String],
                        val outputFilterRegEx: Seq[String]
                       ) extends ProcessRunner {
  type LineTransformer = String => Option[String]

  private val log = LoggerFactory.getLogger(this.getClass)

  private val lastStdoutLines = createCircularBufferOpt()
  private val lastStderrLines = createCircularBufferOpt()

  private var recordCountParsed = Option.empty[Long]
  private var failureFound = false

  private val filterRs = outputFilterRegEx.map(_.r)
  private val recCountR = recordCountRegEx.map(_.r)
  private val zeroRecordsSuccessR = zeroRecordsSuccessRegEx.map(_.r)
  private val failureR = failureRegEx.map(_.r)

  override def run(cmdLine: String): Int = {
    val processBuilder = new ProcessBuilder(StringUtils.tokenize(cmdLine): _*)
      .redirectErrorStream(redirectErrorStream)

    log.info(s"Starting '$cmdLine'")
    val process = processBuilder.start

    val stdout = new BufferedReader(new InputStreamReader(
      process.getInputStream))

    val stderr = new BufferedReader(new InputStreamReader(
      process.getErrorStream))

    processExecutionOutput(stdout, stderr)

    val exitStatus = process.waitFor()

    stdout.close()
    stderr.close()

    if (failureFound && exitStatus == 0) {
      log.info(s"The command contained a message matching the failure expression. The exit status is forced to 1.")
      1
    } else {
      log.info(s"The command completed with exit status $exitStatus.")
      exitStatus
    }
  }

  override def getLastStdoutLines: Array[String] = {
    lastStdoutLines.map(_.get()).getOrElse(Array.empty[String])
  }

  override def getLastStderrLines: Array[String] = {
    lastStderrLines.map(_.get()).getOrElse(Array.empty[String])
  }

  override def recordCount: Option[Long] = recordCountParsed

  private[core] def isFailureFound: Boolean = failureFound

  private def createCircularBufferOpt(): Option[CircularBuffer[String]] = {
    if (includeOutputLines > 0)
      Some(new CircularBuffer[String](includeOutputLines))
    else
      None
  }

  private[core] def processExecutionOutput(stdout: BufferedReader, stderr: BufferedReader): Unit = {
    val futOut = Future {
      processReader(stdout, lastStdoutLines, stdOutLogPrefix, logStdOut)
    }

    val futErr = Future {
      processReader(stderr, lastStderrLines, stdErrLogPrefix, logStdErr)
    }

    Await.ready(Future.sequence(Seq(futOut, futErr)), Duration.Inf)
  }

  private[core] def processReader(reader: BufferedReader,
                                  buffer: Option[CircularBuffer[String]],
                                  prefix: String,
                                  logEnabled: Boolean): Unit = {
    val cmdOutputLogger: Logger = LoggerFactory.getLogger(prefix)
    val lineTransformers = getLineTransformers(buffer, logEnabled, cmdOutputLogger)

    try {
      var line: String = reader.readLine
      while (line != null) {
        lineTransformers.foldLeft(Option(line)) { (lineAcc, f) =>
          lineAcc.flatMap(f)
        }
        line = reader.readLine()
      }
    } catch {
      case NonFatal(ex) => log.error(s"Process stream ($prefix) thrown an exception.", ex)
    }
  }

  private[core] def getLineTransformers(buffer: Option[CircularBuffer[String]],
                                        logEnabled: Boolean,
                                        logger: Logger): Seq[LineTransformer] = {
    Seq(
      // Filters out empty lines
      Some(filterEmpty(_)),
      // Extracts record count
      recCountR.map(regex => extractRecordCount(_: String, regex)),
      // Extracts success with zero records
      zeroRecordsSuccessR.map(regex => extractZeroRecordSuccess(_: String, regex)),
      // Extracts failures
      failureR.map(regex => extractFailure(_: String, regex)),
      // Skips filtered out lines
      Some(skipFilteredOutLines(_: String)),
      // Logs remaining lines
      Some(doIf(_: String, logEnabled)(line => logger.info(line))),
      // Adds remaining lines to the circular buffer
      Some(doIf(_: String, actionNeeded = true)(line => buffer.foreach(_.add(line))))
    ).flatten
  }

  private[core] def doIf(line: String, actionNeeded: Boolean)(action: String => Unit): Option[String] = {
    if (actionNeeded)
      action(line)
    Option(line)
  }

  private[core] def filterEmpty(line: String): Option[String] = {
    if (line.isEmpty)
      None
    else
      Option(line)
  }

  private[core] def skipFilteredOutLines(line: String): Option[String] = {
    val doSkip = filterRs.exists(_.findFirstIn(line).nonEmpty)
    if (doSkip)
      None
    else
      Option(line)
  }

  private[core] def extractRecordCount(line: String, regex: Regex): Option[String] = {
    regex.findFirstMatchIn(line).foreach { matchData =>
      val numberOfRecordsStr = matchData.group(1)
      val recordsOpt = Try(numberOfRecordsStr.toLong) match {
        case Success(number) =>
          log.info(s"Record count match found: $number")
          Some(number)
        case Failure(ex)     =>
          log.error(s"Failed to match the extracted record count '$numberOfRecordsStr' as a number from line '$line', expression: '${regex.regex}'.", ex)
          None
      }
      recordCountParsed = recordsOpt.orElse(recordCountParsed)
    }
    Option(line)
  }

  private[core] def extractZeroRecordSuccess(line: String, regex: Regex): Option[String] = {
    if (recordCountParsed.isEmpty) {
      regex.findFirstMatchIn(line).foreach(_ => recordCountParsed = Some(0))
    }
    Option(line)
  }

  private[core] def extractFailure(line: String, regex: Regex): Option[String] = {
    regex.findFirstMatchIn(line).foreach(_ => failureFound = true)
    Option(line)
  }
}
