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

import za.co.absa.pramen.api.status.{RunStatus, TaskRunReason}

object TestPrototypes {

  val runStatusSuccess: RunStatus = RunStatus.Succeeded(Some(100), Some(200), None, Some(1000), TaskRunReason.New, Seq.empty, Seq.empty, Seq.empty, Seq.empty)

  val runStatusWarning: RunStatus = RunStatus.Succeeded(
    Some(10000), Some(20000), None, Some(100000), TaskRunReason.New, Seq("file1.txt", "file1.ctl"),
    Seq("file1.csv", "file2.csv"), Seq("`db`.`table1`"), Seq("Test warning")
  )

  val runStatusFailure: RunStatus = RunStatus.Failed(new RuntimeException("Test exception"))
}
