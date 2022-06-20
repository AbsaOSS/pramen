/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.tests.state

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.app.config.BookkeeperConfig._
import za.co.absa.pramen.framework.config.WatcherConfig._
import za.co.absa.pramen.framework.mocks.TaskCompletedFactory
import za.co.absa.pramen.framework.state.{RunAppState, RunState}

class RunAppStateSuite extends WordSpec {
  "getExitCode()" should {
    "return 0 on successful jobs" in {
      val state1 = getAppState(isJobSucceeded = true, addFailedNoDataTasks = false, nonZeroCodeOnNoData = false)
      val state2 = getAppState(isJobSucceeded = true, addFailedNoDataTasks = false, nonZeroCodeOnNoData = true)

      assert(state1.getExitCode == 0)
      assert(state2.getExitCode == 0)
    }

    "return 1 on failed jobs" in {
      val state1 = getAppState(isJobSucceeded = false, addFailedNoDataTasks = false, nonZeroCodeOnNoData = false)
      val state2 = getAppState(isJobSucceeded = false, addFailedNoDataTasks = false, nonZeroCodeOnNoData = true)
      val state3 = getAppState(isJobSucceeded = false, addFailedNoDataTasks = true, nonZeroCodeOnNoData = false)
      val state4 = getAppState(isJobSucceeded = false, addFailedNoDataTasks = true, nonZeroCodeOnNoData = true)

      assert(state1.getExitCode == 1)
      assert(state2.getExitCode == 1)
      assert(state3.getExitCode == 1)
      assert(state4.getExitCode == 1)
    }

    "return 0 if there are no data failures but the option is turned off" in {
      val state = getAppState(isJobSucceeded = true, addFailedNoDataTasks = true, nonZeroCodeOnNoData = false)

      assert(state.getExitCode == 0)
    }

    "return 0 if there are no data failures when the option is turned on" in {
      val state = getAppState(isJobSucceeded = true, addFailedNoDataTasks = true, nonZeroCodeOnNoData = true)

      assert(state.getExitCode == 1)
    }
  }

  def getAppState(isJobSucceeded: Boolean, addFailedNoDataTasks: Boolean, nonZeroCodeOnNoData: Boolean): RunState = {
    implicit val conf: Config = ConfigFactory.parseString(
      s"""$BOOKKEEPING_ENABLED = "true"
         |$NON_ZERO_EXIT_CODE_IF_NO_DATA = $nonZeroCodeOnNoData
         |$BOOKKEEPING_CONNECTION_STRING = "mongodb://127.0.0.1"
         |$BOOKKEEPING_DB_NAME = "syncpramen"
         |""".stripMargin
    ).withFallback(ConfigFactory.load())
      .resolve()

    val state = new RunAppState

    state.addCompletedTask(TaskCompletedFactory.getTackCompleted())

    if (addFailedNoDataTasks) {
      state.addCompletedTask(TaskCompletedFactory.getTackCompleted(failureReason = Some("no data")))
    }

    if (isJobSucceeded) {
      state.setSuccess()
    } else {
      state.setFailure(null)
    }

    state
  }

}
