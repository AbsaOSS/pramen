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

package za.co.absa.pramen.core

import za.co.absa.pramen.api.status._
import za.co.absa.pramen.api.{MetaTableDef, SchemaDifference}

import java.time.{Instant, LocalDate}

object TaskNotificationFactory {
  def getDummyTaskNotification(jobName: String = "Dummy Job",
                               outputTable: MetaTableDef = MetaTableDefFactory.getDummyMetaTableDef(name = "dummy_table"),
                               runInfo: Option[RunInfo] = Some(RunInfo(
                                 LocalDate.of(2022, 2, 18),
                                 Instant.ofEpochMilli(1613600000000L),
                                 Instant.ofEpochMilli(1672759508000L)
                               )),
                               status: RunStatus = RunStatus.Succeeded(None, 100, None, TaskRunReason.New, Seq.empty, Seq.empty, Seq.empty, Seq.empty),
                               applicationId: String = "app_12345",
                               isTransient: Boolean = false,
                               isRawFilesJob: Boolean = false,
                               schemaChanges: Seq[SchemaDifference] = Seq.empty,
                               dependencyWarnings: Seq[DependencyWarning] = Seq.empty,
                               notificationTargetErrors: Seq[NotificationFailure] = Seq.empty,
                               options: Map[String, String] = Map.empty[String, String]): TaskResult = {
    TaskResult(jobName,
      outputTable,
      status,
      runInfo,
      applicationId,
      isTransient,
      isRawFilesJob,
      schemaChanges,
      dependencyWarnings,
      notificationTargetErrors,
      options)
  }
}
