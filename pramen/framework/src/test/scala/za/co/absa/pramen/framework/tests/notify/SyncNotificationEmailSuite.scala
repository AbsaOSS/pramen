package za.co.absa.pramen.framework.tests.notify

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.mocks.{NotificationFactory, TaskCompletedFactory}
import za.co.absa.pramen.framework.model.TaskStatus
import za.co.absa.pramen.framework.notify.SyncNotificationEmail

class SyncNotificationEmailSuite extends WordSpec {
  private val notificationSuccess = NotificationFactory.getDummyNotification()

  private val notificationFailure = NotificationFactory.getDummyNotification(
    exception = Some(new RuntimeException("Dummy Exception")),
    tasksCompleted = List(TaskCompletedFactory.getTackCompleted(status = TaskStatus.FAILED.toString))
  )

  private val baseConfig =
    """pramen.warn.throughput.rps = 10
      |pramen.good.throughput.rps = 100
      |pramen.dry.run = false
      |""".stripMargin

  "getFrom" should {
    "return the 'from' list from the config" in {
      implicit val conf: Config = ConfigFactory.parseString(
        baseConfig + "mail.send.from = \"test@email\""
      )

      val emailNotification = new SyncNotificationEmail(notificationSuccess)

      assert(emailNotification.getFrom == "test@email")
    }
  }

  "getTo" should {
    "return the 'to' list from the config when there are no failures" in {
      implicit val conf: Config = ConfigFactory.parseString(
        baseConfig + """mail.send.from = "test@email.from"
          |mail.send.to = "test@email.to"
          |mail.send.failures.to = "test@email.failures.to"
          |""".stripMargin)

      val emailNotification = new SyncNotificationEmail(notificationSuccess)

      assert(emailNotification.getTo == "test@email.to")
    }

    "return the 'to' list from the config when failure list is not defined" in {
      implicit val conf: Config = ConfigFactory.parseString(
        baseConfig + """mail.send.from = "test@email.from"
          |mail.send.to = "test@email.to"
          |""".stripMargin)

      val emailNotification = new SyncNotificationEmail(notificationFailure)

      assert(emailNotification.getTo == "test@email.to")
    }

    "return the 'fails.to' list if there are failures and the failure list is defined" in {
      implicit val conf: Config = ConfigFactory.parseString(
        baseConfig + """mail.send.from = "test@email.from"
          |mail.send.to = "test@email.to"
          |mail.send.failures.to = "test@email.failures.to"
          |""".stripMargin)

      val emailNotification = new SyncNotificationEmail(notificationFailure)

      assert(emailNotification.getTo == "test@email.failures.to")
    }
  }

}
