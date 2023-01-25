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

trait ProcessRunner {
  /**
    * Runs the process.
    *
    * @param cmdLine The command line to run.
    * @return the exit code.
    */
  def run(cmdLine: String): Int

  /**
    * Returns last lines written to stdout.
    *
    * The number of lines to return is determined by a configuration key.
    *
    * @return stdout lines.
    */
  def getLastStdoutLines: Array[String]

  /**
    * Returns last lines written to stderr.
    *
    * The number of lines to return is determined by a configuration key.
    *
    * @return stdout lines.
    */
  def getLastStderrLines: Array[String]

  /**
    * Returns the number of records written if such an expression is specified and encountered.
    *
    * @return records written (if available).
    */
  def recordCount: Option[Long]
}

object ProcessRunner {
  def apply(includeOutputLines: Int,
            logStdOut: Boolean = true,
            logStdErr: Boolean = true,
            stdOutLogPrefix: String = "CmdOut",
            stdErrLogPrefix: String = "CmdErr",
            redirectErrorStream: Boolean = false,
            recordCountRegEx: Option[String] = None,
            zeroRecordsSuccessRegEx: Option[String] = None,
            failureRegEx: Option[String] = None,
            outputFilterRegEx: Seq[String] = Seq.empty[String]): ProcessRunner = {
    new ProcessRunnerImpl(includeOutputLines,
      logStdOut,
      logStdErr,
      stdOutLogPrefix,
      stdErrLogPrefix,
      redirectErrorStream,
      recordCountRegEx,
      zeroRecordsSuccessRegEx,
      failureRegEx,
      outputFilterRegEx)
  }
}
