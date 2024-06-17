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
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api.status.TaskResult
import za.co.absa.pramen.api.{ExternalChannelFactory, NotificationTarget, PipelineInfo}

import scala.collection.mutable.ListBuffer

class NotificationTargetSpy(conf: Config, action: TaskResult => Unit) extends NotificationTarget {
  val notificationsSent: ListBuffer[TaskResult] = new ListBuffer[TaskResult]()

  override def config: Config = conf

  override def sendNotification(pipelineInfo: PipelineInfo, notification: TaskResult): Unit = {
    notificationsSent += notification
    action(notification)
  }
}

object NotificationTargetSpy extends ExternalChannelFactory[NotificationTargetSpy] {
  override def apply(conf: Config, parentPath: String, spark: SparkSession): NotificationTargetSpy = {
    new NotificationTargetSpy(conf, _ => ())
  }
}
