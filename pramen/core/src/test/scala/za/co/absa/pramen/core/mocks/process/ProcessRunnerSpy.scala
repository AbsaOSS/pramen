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

package za.co.absa.pramen.core.mocks.process

import za.co.absa.pramen.core.process.ProcessRunner

import scala.collection.mutable.ListBuffer

class ProcessRunnerSpy(exitCode: Int = 0,
                       stdOutLines: Array[String] = new Array[String](0),
                       stdErrLines: Array[String] = new Array[String](0),
                       runException: Throwable = null,
                       runFunction: () => Unit = () => {},
                       recordCountToReturn: Option[Long] = None) extends ProcessRunner {
  var runCommands = new ListBuffer[String]
  var getLastStdoutLinesCount = 0
  var getLastStderrLinesCount = 0
  var recordCountCalled = 0

  override def run(cmdLine: String): Int = {
    runCommands += cmdLine

    if (runException != null) {
      throw runException
    }

    runFunction()

    exitCode
  }

  override def getLastStdoutLines: Array[String] = {
    getLastStdoutLinesCount += 1
    stdOutLines
  }

  override def getLastStderrLines: Array[String] = {
    getLastStderrLinesCount += 1
    stdErrLines
  }

  override def recordCount: Option[Long] = {
    recordCountCalled += 1

    recordCountToReturn
  }
}
