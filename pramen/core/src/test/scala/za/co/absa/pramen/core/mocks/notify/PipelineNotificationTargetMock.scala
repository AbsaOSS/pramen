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

package za.co.absa.pramen.core.mocks.notify

import com.typesafe.config.Config
import za.co.absa.pramen.api.status.{CustomNotification, TaskResult}
import za.co.absa.pramen.api.{PipelineInfo, PipelineNotificationTarget}
import za.co.absa.pramen.core.mocks.notify.NotificationTargetMock.TEST_NOTIFICATION_FAIL_KEY

class PipelineNotificationTargetMock(conf: Config) extends PipelineNotificationTarget {

  override def sendNotification(pipelineInfo: PipelineInfo,
                                tasksCompleted: Seq[TaskResult],
                                customNotification: CustomNotification): Unit = {
    if (conf.hasPath(TEST_NOTIFICATION_FAIL_KEY) && conf.getBoolean(TEST_NOTIFICATION_FAIL_KEY)) {
      System.setProperty("pramen.test.notification.pipeline.failure", "true")
      throw new RuntimeException("Pipeline notification target test exception")
    }

    System.setProperty("pramen.test.notification.tasks.completed", tasksCompleted.length.toString)
  }

  override def config: Config = conf
}
