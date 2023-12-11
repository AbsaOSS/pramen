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

package za.co.absa.pramen.core.notify

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.notification.NotificationEntry
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.Emoji._

import java.util.Properties
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.{Message, Session, Transport}
import scala.util.control.NonFatal

object Sendable {
  // Configuration keys
  val MAIL_SEND_FROM_KEY = "mail.send.from"
  val MAIL_SEND_TO_KEY = "mail.send.to"

  val MAIL_SMTP_HOST_KEY = "mail.smtp.host"
  val MAIL_SMTP_PORT_KEY = "mail.smtp.port"
  val MAIL_SMTP_AUTH_KEY = "mail.smtp.auth"
  val MAIL_SMTP_LOCALHOST = "mail.smtp.localhost"
  val MAIL_SMTP_STARTTLS_ENABLE_KEY = "mail.smtp.starttls.enable"
  val MAIL_SMTP_SSL_ENABLE_KEY = "mail.smtp.EnableSSL.enable"
  val MAIL_DEBUG_KEY = "mail.debug"
}

trait Sendable {
  import Sendable._

  private val log = LoggerFactory.getLogger(this.getClass)

  def getConfig: Config

  def getEncoding: String = "UTF-8"

  def getFormat: String = "html"

  def getFrom: String = getConfig.getString(MAIL_SEND_FROM_KEY)

  def getTo: String = getConfig.getString(MAIL_SEND_TO_KEY)

  def getSubject: String

  def getBody: String

  def getFiles: Seq[NotificationEntry.AttachedFile] = Seq()

  final def send(): Unit = {
    if (getConfig.hasPath(MAIL_SEND_TO_KEY) && getConfig.getString(MAIL_SEND_TO_KEY).trim.nonEmpty) {
      doSend()
    } else {
      log.info(s"No senders are configured at ($MAIL_SEND_TO_KEY). The notification email won't be sent.")
    }
  }

  final def doSend(): Unit = {
    implicit val props: Properties = getMailProperties

    resetProperty(MAIL_DEBUG_KEY)

    val session = Session.getInstance(props)
    val message = new MimeMessage(session)

    // Set the contents of the email
    message.setFrom(new InternetAddress(getFrom))
    message.setRecipients(Message.RecipientType.TO, getTo)
    message.setSubject(getSubject)
    message.setText(getBody, getEncoding, getFormat)

    if (getFiles.nonEmpty) {
      setFiles(message)
    }

    // Send it
    try {
      Transport.send(message)
      log.info(s"$VOLTAGE An email has been sent successfully.")
    } catch {
      case NonFatal(ex) => log.error(s"$FAILURE Failed to send an email.", ex)
    }
  }

  private def setFiles(message: MimeMessage): Unit = {
    val multipart = new MimeMultipart()

    // Attach the message body
    val messageBodyPart = new MimeBodyPart()
    messageBodyPart.setText(getBody, getEncoding, getFormat)
    multipart.addBodyPart(messageBodyPart)

    // Attach the files if any
    attachFiles(getFiles, multipart)

    message.setContent(multipart)
  }

  private def attachFiles(files: Seq[NotificationEntry.AttachedFile], multipart: MimeMultipart): Unit = {
    files.foreach { file =>
      val attachmentBodyPart = new MimeBodyPart()
      attachmentBodyPart.setContent(file.contents, "application/octet-stream")
      attachmentBodyPart.setFileName(file.fileName)
      multipart.addBodyPart(attachmentBodyPart)
    }
  }

  private def resetProperty(propertyName: String)(implicit props: Properties): Unit = {
    props.setProperty(propertyName, getConfig.getString(propertyName))
  }

  private def getMailProperties: Properties = {
    val props = System.getProperties

    val configMap = ConfigUtils.getFlatConfig(getConfig.getConfig("mail.smtp"))

    configMap.foreach{ case (k, v) =>
      props.setProperty(s"mail.smtp.$k", v.toString)
    }

    props
  }
}
