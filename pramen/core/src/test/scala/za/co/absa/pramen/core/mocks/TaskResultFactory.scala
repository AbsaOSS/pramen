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

import za.co.absa.pramen.api.status._
import za.co.absa.pramen.api.{MetaTableDef, SchemaDifference}
import za.co.absa.pramen.core.metastore.model.MetaTable

import java.time.{Instant, LocalDate}

object TaskResultFactory {
  def getDummyTaskResult(jobName: String = "DummyJob",
                         outputTable: MetaTableDef = MetaTable.getMetaTableDef(MetaTableFactory.getDummyMetaTable(name = "table_out")),
                         runStatus: RunStatus = RunStatus.Succeeded(Some(100), 200, Some(1000), TaskRunReason.New, Nil, Nil, Nil, Nil),
                         runInfo: Option[RunInfo] = Some(RunInfo(LocalDate.of(2022, 2, 18), Instant.ofEpochSecond(1234), Instant.ofEpochSecond(5678))),
                         applicationId: String = "app_123",
                         isTransient: Boolean = false,
                         isRawFilesJob: Boolean = false,
                         schemaDifferences: Seq[SchemaDifference] = Nil,
                         dependencyWarnings: Seq[DependencyWarning] = Nil,
                         notificationTargetErrors: Seq[NotificationFailure] = Nil,
                         options: Map[String, String] = Map.empty): TaskResult = {
    TaskResult(jobName,
      outputTable,
      runStatus,
      runInfo,
      applicationId,
      isTransient,
      isRawFilesJob,
      schemaDifferences,
      dependencyWarnings,
      notificationTargetErrors,
      options)
  }

}
