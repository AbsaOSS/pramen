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

package za.co.absa.pramen.framework.process

import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.pramen.framework.utils.{CircularBuffer, StringUtils}

import java.io.{BufferedReader, InputStreamReader}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

class ProcessRunnerImpl(includeOutputLines: Int,
                        logStdOut: Boolean,
                        logStdErr: Boolean,
                        stdOutLogPrefix: String,
                        stdErrLogPrefix: String,
                        redirectErrorStream: Boolean) extends ProcessRunner {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val lastStdoutLines = createCircularBufferOpt()
  private val lastStderrLines = createCircularBufferOpt()

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

    log.info(s"The command completed with exit status $exitStatus.")
    exitStatus
  }

  override def getLastStdoutLines: Array[String] = {
    lastStdoutLines.map(_.get()).getOrElse(Array.empty[String])
  }

  override def getLastStderrLines: Array[String] = {
    lastStderrLines.map(_.get()).getOrElse(Array.empty[String])
  }

  private def createCircularBufferOpt(): Option[CircularBuffer[String]] = {
    if (includeOutputLines > 0)
      Some(new CircularBuffer[String](includeOutputLines))
    else
      None
  }

  private[framework] def processExecutionOutput(stdout: BufferedReader, stderr: BufferedReader): Unit = {
    val futOut = Future {
      processReader(stdout, lastStdoutLines, stdOutLogPrefix, logStdOut)
    }

    val futErr = Future {
      processReader(stderr, lastStderrLines, stdErrLogPrefix, logStdErr)
    }

    Await.ready(Future.sequence(Seq(futOut, futErr)), Duration.Inf)
  }

  private[framework] def processReader(reader: BufferedReader,
                            buffer: Option[CircularBuffer[String]],
                            prefix: String,
                            logEnabled: Boolean): Unit = {
    val cmdOutputLogger: Logger = LoggerFactory.getLogger(prefix)

    try {
      var line: String = reader.readLine
      while (line != null) {
        if (line.nonEmpty) {
          buffer.foreach(_.add(line))
          if (logEnabled) {
            cmdOutputLogger.info(line)
          }
        }
        line = reader.readLine()
      }
    } catch {
      case NonFatal(ex) => log.error(s"Process stream ($prefix) thrown an exception.", ex)
    }
  }
}
