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

package za.co.absa.pramen.core.mocks

import za.co.absa.pramen.core.process.ProcessRunnerImpl

class DummyProcessRunner(includeOutputLines: Int = 100,
                         logStdOut: Boolean = true,
                         logStdErr: Boolean = true,
                         redirectErrorStream: Boolean = false,
                         recordCountRegEx: Option[String] = None,
                         zeroRecordsSuccessRegEx: Option[String] = None,
                         failureRegEx: Option[String] = None,
                         outputFilterRegEx: Seq[String] = Nil)
  extends ProcessRunnerImpl(includeOutputLines,
    logStdOut,
    logStdErr,
    "CmdOut",
    "CmdErr",
    redirectErrorStream,
    recordCountRegEx,
    zeroRecordsSuccessRegEx,
    failureRegEx,
    outputFilterRegEx) {

  override def run(cmdLine: String): Int = {
    throw new IllegalStateException(s"Cannot run a dummy process.")
  }

}
