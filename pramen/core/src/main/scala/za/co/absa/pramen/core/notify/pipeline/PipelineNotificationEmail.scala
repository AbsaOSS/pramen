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
import za.co.absa.pramen.core.utils.{ConfigUtils, Emoji}

import scala.collection.mutable.ListBuffer

class PipelineNotificationEmail(notification: PipelineNotification)
                               (implicit conf: Config) extends Sendable {

  import PipelineNotificationEmail._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val allowedDomains = getDomainList(ConfigUtils.getOptListStrings(conf, Keys.MAIL_ALLOWED_DOMAINS))

  private lazy val notificationBuilder = {
    val builder = new PipelineNotificationBuilderHtml

    PipelineNotificationDirector.build(builder, notification, validateRecipientEmails(getEmailRecipients, allowedDomains))
    builder
  }

  override def getConfig: Config = conf

  override def getFrom: String = conf.getString(Keys.MAIL_FROM)

  override def getTo: String = {
    val validatedEmails = validateRecipientEmails(getEmailRecipients, allowedDomains)

    validatedEmails.invalidFormatEmails.foreach(email =>
      log.error(s"${Emoji.FAILURE} Invalid email format: $email")
    )

    validatedEmails.invalidDomainEmails.foreach(email =>
      log.error(s"${Emoji.FAILURE} Invalid email domain: $email")
    )

    validatedEmails.validEmails.mkString(", ")
  }

  override def getSubject: String = {
    notificationBuilder.renderSubject()
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

  private[core] def getEmailRecipients: String = {
    if (conf.hasPath(Keys.MAIL_FAILURES_TO)) {
      if (notification.exception.isDefined || notification.tasksCompleted.exists(t => t.runStatus.isFailure)) {
        val to = conf.getString(Keys.MAIL_FAILURES_TO)
        log.warn(s"Sending failures to the special mail list: $to")
        to
      } else {
        val to = conf.getString(Keys.MAIL_TO)
        log.info(s"Sending success to the normal mail list: $to")
        to
      }
    } else {
      val to = conf.getString(Keys.MAIL_TO)
      log.info(s"Sending the notification to: $to")
      to
    }
  }
}

object PipelineNotificationEmail {
  def validateRecipientEmails(emails: String, allowedDomains: Seq[String]): ValidatedEmails = {
    val allEmails = splitEmails(emails)

    val validEmails = new ListBuffer[String]
    val invalidFormatEmails = new ListBuffer[String]
    val invalidDomainEmails = new ListBuffer[String]

    allEmails.foreach { email =>
      if (isEmailProperlyFormed(email)) {
        if (isEmailDomainAllowed(email, allowedDomains)) {
          validEmails += email
        } else {
          invalidDomainEmails += email
        }
      } else {
        invalidFormatEmails += email
      }
    }

    ValidatedEmails(validEmails, invalidFormatEmails, invalidDomainEmails)
  }

  private[core] def getDomainList(domains: Seq[String]): Seq[String] = {
    domains.map(_.toLowerCase().trim())
      .map { domain =>
        if (domain.startsWith("@"))
          domain.tail
        else
          domain
      }
  }

  private[core] def splitEmails(emails: String): Seq[String] = {
    emails.split("[,;]").map(_.trim).filter(_.nonEmpty)
  }

  private[core] def isEmailProperlyFormed(email: String): Boolean = {
    val emailRegex = "^[^@]+@[^@]+$".r
    emailRegex.findFirstMatchIn(email).isDefined
  }

  private[core] def isEmailDomainAllowed(email: String, allowedDomains: Seq[String]): Boolean = {
    if (allowedDomains.isEmpty) {
      true
    } else {
      val domain = email.split("@").last.toLowerCase
      allowedDomains.contains(domain)
    }
  }
}
