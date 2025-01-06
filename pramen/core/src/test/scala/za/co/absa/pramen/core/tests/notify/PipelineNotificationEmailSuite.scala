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

package za.co.absa.pramen.core.tests.notify

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.mocks.PipelineNotificationFactory
import za.co.absa.pramen.core.notify.pipeline.PipelineNotificationEmail

class PipelineNotificationEmailSuite extends AnyWordSpec {
  "getConfig" should {
    "returns the config passed to the constructor" in {
      val pipelineNotificationEmail = getUseCase()
      val conf = pipelineNotificationEmail.getConfig

      assert(conf.getString("test.key") == "test.value")
    }
  }

  "getFrom" should {
    "return the configured sender from config" in {
      val pipelineNotificationEmail = getUseCase()
      val sender = pipelineNotificationEmail.getFrom

      assert(sender == "Pramen <noreply@example.com>")
    }
  }

  "getTo" should {
    "return the input email recipients from the config" in {
      val pipelineNotificationEmail = getUseCase()
      val recipients = pipelineNotificationEmail.getTo

      assert(recipients == "test@example.com")
    }

    "return the input email recipients if allowed domain list is empty" in {
      val pipelineNotificationEmail = getUseCase(allowedDomains = Seq.empty)
      val recipients = pipelineNotificationEmail.getTo

      assert(recipients == "test@example.com")
    }

    "not return emails that are not in allowed domains" in {
      val pipelineNotificationEmail = getUseCase(allowedDomains = Seq("test.com"))
      val recipients = pipelineNotificationEmail.getTo

      assert(recipients == "")
    }

    "not return improperly formed emails" in {
      val pipelineNotificationEmail = getUseCase(mailTo = "abc123", allowedDomains = Seq.empty)
      val recipients = pipelineNotificationEmail.getTo

      assert(recipients == "")
    }
  }

  "getSubject" should {
    "return the appropriate subject" in {
      val pipelineNotificationEmail = getUseCase()

      assert(pipelineNotificationEmail.getSubject.startsWith("Notification of SUCCESS for DummyPipeline"))
    }
  }

  "getBody" should {
    "return the body of the email" in {
      val pipelineNotificationEmail = getUseCase()

      assert(pipelineNotificationEmail.getBody.contains("<p>This is a notification from Pramen"))
    }
  }

  "getEmailRecipients" should {
    "return the input email recipients from the config" in {
      val pipelineNotificationEmail = getUseCase()

      val recipients = pipelineNotificationEmail.getEmailRecipients

      assert(recipients == "test@example.com")
    }

    "return emails even iof they are not in the allowed domains" in {
      val pipelineNotificationEmail = getUseCase(allowedDomains = Seq("test.com"))
      val recipients = pipelineNotificationEmail.getEmailRecipients

      assert(recipients == "test@example.com")
    }

    "return the failure email recipients from the config" in {
      val pipelineNotificationEmail = getUseCase(isFailure = true, failureEmailRecipients = "failure@test.com")

      val recipients = pipelineNotificationEmail.getEmailRecipients

      assert(recipients == "failure@test.com")
    }

    "return the success email recipients if the app succeeded" in {
      val pipelineNotificationEmail = getUseCase(failureEmailRecipients = "failure@test.com")

      val recipients = pipelineNotificationEmail.getEmailRecipients

      assert(recipients == "test@example.com")
    }
  }

  "getDomainList" should {
    "remove @ from domain names if specified" in {
      val domains = Seq("test.com", "@example.COM")

      val actual = PipelineNotificationEmail.getDomainList(domains)

      assert(actual.length == 2)
      assert(actual.head == "test.com")
      assert(actual(1) == "example.com")
    }
  }

  "splitEmails" should {
    "support empty email list" in {
      val emails = "  "

      val actual = PipelineNotificationEmail.splitEmails(emails)

      assert(actual.isEmpty)
    }

    "support empty email list 2" in {
      val emails = " ; "

      val actual = PipelineNotificationEmail.splitEmails(emails)

      assert(actual.isEmpty)
    }

    "split comma-separated emails" in {
      val emails = " email1@test.com , email2@example.com "

      val actual = PipelineNotificationEmail.splitEmails(emails)

      assert(actual.length == 2)
      assert(actual.head == "email1@test.com")
      assert(actual(1) == "email2@example.com")
    }

    "split semicolon-separated emails" in {
      val emails = " email1@test.com ; email2@example.com "

      val actual = PipelineNotificationEmail.splitEmails(emails)

      assert(actual.length == 2)
      assert(actual.head == "email1@test.com")
      assert(actual(1) == "email2@example.com")
    }

    "split mixed-separated emails" in {
      val emails = " email1@test.com ; email2@example.com , email3@example.com "

      val actual = PipelineNotificationEmail.splitEmails(emails)

      assert(actual.length == 3)
      assert(actual.head == "email1@test.com")
      assert(actual(1) == "email2@example.com")
      assert(actual(2) == "email3@example.com")
    }
  }

  "isEmailProperlyFormed" should {
    "return true for valid emails" in {
      assert(PipelineNotificationEmail.isEmailProperlyFormed("test@examile.com"))
      assert(PipelineNotificationEmail.isEmailProperlyFormed("123test_%+-345.abc1234@TEST123.COM"))
      assert(PipelineNotificationEmail.isEmailProperlyFormed("123test_%+-345.abc1234@ABSA.CO1.ZA"))
    }
    "return false for invalid emails" in {
      assert(!PipelineNotificationEmail.isEmailProperlyFormed("test_examile.com"))
      assert(!PipelineNotificationEmail.isEmailProperlyFormed("123test_%+345@.abc1234@TEST123.COM"))
      assert(!PipelineNotificationEmail.isEmailProperlyFormed("123test_%@345.abc1234@ABSA.CO1.ZA"))
    }
  }

  "isEmailDomainAllowed" should {
    "return true for empty allowed domains" in {
      assert(PipelineNotificationEmail.isEmailDomainAllowed("test1@test.com", Seq.empty))
      assert(PipelineNotificationEmail.isEmailDomainAllowed("123test_%+-345.abc1234@TEST123.COM", Seq.empty))
      assert(PipelineNotificationEmail.isEmailDomainAllowed("123test_%+-345.abc1234@ABSA.CO1.ZA", Seq.empty))
    }

    "return true for allowed domains" in {
      assert(PipelineNotificationEmail.isEmailDomainAllowed("test1@test.com", Seq("test.com")))
      assert(PipelineNotificationEmail.isEmailDomainAllowed("123test_%+-345.abc1234@TEST123.COM", Seq("test123.com", "test.com")))
      assert(PipelineNotificationEmail.isEmailDomainAllowed("123test_%+-345.abc1234@ABSA.CO.ZA", Seq("absa.co.za")))
    }

    "return true for not allowed domains" in {
      assert(!PipelineNotificationEmail.isEmailDomainAllowed("test1@test.com", Seq("test123.com")))
      assert(!PipelineNotificationEmail.isEmailDomainAllowed("test@TEST123.COM", Seq("test456.com", "test.com")))
      assert(!PipelineNotificationEmail.isEmailDomainAllowed("test@ABSA.CO1.ZA", Seq("absa.co.za")))
    }
  }

  "validateRecipientEmails" should {
    "split email list between valid and non valid ones" in {
      val emails = "test1@test.com, abc1234@INVALID.COM, invalid+email, test@absa.co.za"

      val actual = PipelineNotificationEmail.validateRecipientEmails(emails, Seq("test.com", "absa.co.za"))

      assert(actual.validEmails.length == 2)
      assert(actual.validEmails.head == "test1@test.com")
      assert(actual.validEmails(1) == "test@absa.co.za")
      assert(actual.invalidFormatEmails.length == 1)
      assert(actual.invalidFormatEmails.head == "invalid+email")
      assert(actual.invalidDomainEmails.length == 1)
      assert(actual.invalidDomainEmails.head == "abc1234@INVALID.COM")
    }
  }

  def getUseCase(
                  mailTo: String = "test@example.com",
                  allowedDomains: Seq[String] = Seq("@example.com"),
                  isFailure: Boolean = false,
                  failureEmailRecipients: String = ""
                ): PipelineNotificationEmail = {
    import za.co.absa.pramen.core.config.Keys._

    val appException = if (isFailure) Some(new RuntimeException("test exception")) else None
    val notification = PipelineNotificationFactory.getDummyNotification(exception = appException)

    val failureEmailStr = if (failureEmailRecipients.isEmpty) "" else s"""$MAIL_FAILURES_TO = "$failureEmailRecipients""""
    implicit val conf: Config = ConfigFactory.parseString(
      s"""test.key = "test.value"
         |$MAIL_FROM = "Pramen <noreply@example.com>"
         |$MAIL_TO = "$mailTo"
         |$failureEmailStr
         |$MAIL_ALLOWED_DOMAINS = [ ${allowedDomains.map(s => s""""$s"""").mkString(", ")} ]
         |""".stripMargin
    ).withFallback(ConfigFactory.load())

    new PipelineNotificationEmail(notification)
  }
}
