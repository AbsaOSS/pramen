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

package za.co.absa.pramen.core.notify.pipeline

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.notification.NotificationEntry
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.notify.Sendable

class PipelineNotificationEmail(notification: PipelineNotification)
                               (implicit conf: Config) extends Sendable {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val notificationBuilder = new PipelineNotificationBuilderHtml

  PipelineNotificationDirector.build(notificationBuilder, notification)

  def getSubject: String = {
    notificationBuilder.renderSubject()
  }

  override def getConfig: Config = conf

  override def getFrom: String = conf.getString(Keys.MAIL_FROM)

  override def getTo: String = {
    if (conf.hasPath(Keys.MAIL_FAILURES_TO)) {
      if (notification.exception.isDefined || notification.tasksCompleted.exists(t => t.runStatus.isFailure)) {
        val to = conf.getString(Keys.MAIL_FAILURES_TO)
        log.warn(s"Sending failures to the special mail list: $to")
        to
      } else {
        val to = conf.getString(Keys.MAIL_TO)
        log.warn(s"Sending success to the normal mail list: $to")
        to
      }
    } else {
      val to = conf.getString(Keys.MAIL_TO)
      log.info(s"Sending the notification to: $to")
      to
    }
  }

  override def getBody: String = {
    notificationBuilder.renderBody()
  }

  override def getFiles: Seq[NotificationEntry.AttachedFile] = {
    notificationBuilder.customEntries.flatMap {
      case NotificationEntry.AttachedFile(name, content) => Some(NotificationEntry.AttachedFile(name, content))
      case _ => None
    }.toSeq
  }
}
