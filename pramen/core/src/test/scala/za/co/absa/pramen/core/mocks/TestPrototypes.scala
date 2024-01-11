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

import za.co.absa.pramen.core.pipeline.TaskRunReason
import za.co.absa.pramen.core.runner.task.RunStatus

object TestPrototypes {

  val runStatusSuccess: RunStatus = RunStatus.Succeeded(Some(100), 200, Some(1000), TaskRunReason.New, Seq.empty, Seq.empty, Seq.empty, Seq.empty)

  val runStatusWarning: RunStatus = RunStatus.Succeeded(
    Some(10000), 20000, Some(100000), TaskRunReason.New, Seq("file1.txt", "file1.ctl"),
    Seq("file1.csv", "file2.csv"), Seq("`db`.`table1`"), Seq("Test warning")
  )

  val runStatusFailure: RunStatus = RunStatus.Failed(new RuntimeException("Test exception"))
}
