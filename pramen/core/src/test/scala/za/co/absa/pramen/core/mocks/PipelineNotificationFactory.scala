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

import za.co.absa.pramen.api.notification.{NotificationEntry, TextElement}
import za.co.absa.pramen.api.status.{PipelineNotificationFailure, TaskResult}
import za.co.absa.pramen.core.notify.pipeline
import za.co.absa.pramen.core.notify.pipeline.PipelineNotification

import java.time.Instant

object PipelineNotificationFactory {
  def getDummyNotification(exception: Option[Throwable] = None,
                           warningFlag: Boolean = false,
                           pipelineName: String = "DummyPipeline",
                           environmentName: String = "DummyEnvironment",
                           sparkAppId: Option[String] = None,
                           started: Instant = Instant.ofEpochSecond(1234567L),
                           finished: Instant = Instant.ofEpochSecond(1234568L),
                           tasksCompleted: List[TaskResult] = List(TaskResultFactory.getDummyTaskResult()),
                           pipelineNotificationFailures: List[PipelineNotificationFailure] = List.empty,
                           customEntries: List[NotificationEntry] = List.empty[NotificationEntry],
                           customSignature: List[TextElement] = List.empty[TextElement]
                          ): PipelineNotification = {
    pipeline.PipelineNotification(
      exception,
      warningFlag,
      pipelineName,
      environmentName,
      sparkAppId,
      started,
      finished,
      tasksCompleted,
      pipelineNotificationFailures,
      customEntries,
      customSignature
    )
  }

}
