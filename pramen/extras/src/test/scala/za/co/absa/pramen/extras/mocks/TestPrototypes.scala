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

package za.co.absa.pramen.extras.mocks

import za.co.absa.pramen.api.status.{RunInfo, RunStatus, TaskResult, TaskRunReason}
import za.co.absa.pramen.api.{DataFormat, MetaTableDef, status}
import za.co.absa.pramen.extras.utils.httpclient.SimpleHttpResponse

import java.time.{Instant, LocalDate}

object TestPrototypes {
  val infoDate: LocalDate = LocalDate.of(2022, 2, 18)

  val httpResponse: SimpleHttpResponse = SimpleHttpResponse(200, None, Seq.empty)

  val metaTableDef: MetaTableDef = MetaTableDef(
    "table1",
    "",
    DataFormat.Null(),
    "pramen_info_date",
    "yyyy-MM-dd",
    None,
    None,
    LocalDate.MIN,
    Map.empty,
    Map.empty)

  val taskStatus: RunStatus = RunStatus.Succeeded(None, 100, None, TaskRunReason.New, Seq.empty, Seq.empty, Seq.empty, Seq.empty)

  val taskNotification: TaskResult = status.TaskResult(
    "Dummy Job",
    metaTableDef,
    taskStatus,
    Option(RunInfo(infoDate, Instant.ofEpochSecond(1645274606), Instant.ofEpochSecond(1645278206))),
    "test-1234",
    isTransient = false,
    isRawFilesJob = false,
    Seq.empty,
    Seq.empty,
    Seq.empty,
    Map.empty)
}
