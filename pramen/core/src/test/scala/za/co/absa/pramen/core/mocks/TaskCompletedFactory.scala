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

import za.co.absa.pramen.api.status.TaskStatus
import za.co.absa.pramen.core.journal.model
import za.co.absa.pramen.core.journal.model.TaskCompleted

import java.time.LocalDate

object TaskCompletedFactory {
  def getTaskCompleted(jobName: String = "DummyJob",
                       tableName: String = "DummyTable",
                       periodBegin: LocalDate = LocalDate.of(2020, 12, 9),
                       periodEnd: LocalDate = LocalDate.of(2020, 12, 9),
                       informationDate: LocalDate = LocalDate.of(2020, 12, 9),
                       inputRecordCount: Long = 1000L,
                       inputRecordCountOld: Long = 1000L,
                       outputRecordCount: Option[Long] = Some(1000L),
                       outputRecordCountOld: Option[Long] = Some(1000L),
                       outputSize: Option[Long] = None,
                       startedAt: Long = 1234567L,
                       finishedAt: Long = 1234568L,
                       status: String = TaskStatus.NEW.toString,
                       failureReason: Option[String] = None,
                       sparkApplicationId: Option[String] = Some("abc123"),
                       pipelineId: Option[String] = Some(java.util.UUID.randomUUID().toString),
                       pipelineName: Option[String] = Some("test"),
                       environmentName: Option[String] = Some("DEV"),
                       tenant: Option[String] = Some("Dummy tenant")): TaskCompleted = {
    model.TaskCompleted(
      jobName,
      tableName,
      periodBegin,
      periodEnd,
      informationDate,
      inputRecordCount,
      inputRecordCountOld,
      outputRecordCount,
      outputRecordCountOld,
      outputSize,
      startedAt,
      finishedAt,
      status,
      failureReason,
      sparkApplicationId,
      pipelineId,
      pipelineName,
      environmentName,
      tenant
    )
  }

}
