package za.co.absa.pramen.core.tests.notify

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.TaskStatus
import za.co.absa.pramen.core.notify.NotificationTargetManager
import za.co.absa.pramen.core.pipeline.DependencyFailure
import za.co.absa.pramen.core.runner.task.RunStatus

class NotificationTargetManagerSuite extends AnyWordSpec {
  private val conf: Config = ConfigFactory.parseString(
    s"""
       | pramen.notification.targets = [
       |    {
       |      name = "hyperdrive1"
       |      factory.class = "za.co.absa.pramen.core.notify.HyperdriveNotificationTarget"
       |
       |      kafka.topic = "mytopic"
       |
       |      kafka.option {
       |         bootstrap.servers = "dummy:9092,dummy:9093"
       |         sasl.mechanism = "GSSAPI"
       |         security.protocol = "SASL_SSL"
       |      }
       |    }
       |  ]
       |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  "getByName()" should {

  }

  "runStatusToTaskStatus" should {
    "decode success" in {
      assert(NotificationTargetManager.runStatusToTaskStatus(RunStatus.Succeeded(None, 100, None, null)).contains(TaskStatus.Succeeded(100)))
    }

    "decode failure" in {
      val ex = new RuntimeException("dummy")
      assert(NotificationTargetManager.runStatusToTaskStatus(RunStatus.Failed(ex)).contains(TaskStatus.Failed(ex)))
    }

    "decode validation failure" in {
      val ex = new RuntimeException("dummy")
      assert(NotificationTargetManager.runStatusToTaskStatus(RunStatus.ValidationFailed(ex)).contains(TaskStatus.ValidationFailed(ex)))
    }

    "decode missing dependencies" in {
      val tables = Seq("table1", "table2")

      assert(NotificationTargetManager.runStatusToTaskStatus(RunStatus.MissingDependencies(tables)).contains(TaskStatus.MissingDependencies(tables)))
    }

    "decode failed dependencies" in {
      val failedDependencies = Seq(
        DependencyFailure(null, Seq("table0"), Seq("table1", "table2"), null),
        DependencyFailure(null, Seq("table99"), Seq("table3", "table1"), null)
      )

      val failedTables = Seq("table0", "table1", "table2", "table3", "table99")

      assert(NotificationTargetManager.runStatusToTaskStatus(RunStatus.FailedDependencies(failedDependencies)).contains(TaskStatus.FailedDependencies(failedTables)))
    }

    "decode no data" in {
      assert(NotificationTargetManager.runStatusToTaskStatus(RunStatus.NoData).contains(TaskStatus.NoData))
    }

    "decode insufficient data" in {
      assert(NotificationTargetManager.runStatusToTaskStatus(RunStatus.InsufficientData(100, 200, None)).contains(TaskStatus.InsufficientData(100, 200)))
    }

    "decode skipped" in {
      assert(NotificationTargetManager.runStatusToTaskStatus(RunStatus.Skipped("msg")).contains(TaskStatus.Skipped("msg")))
    }

    "decode not ran" in {
      assert(NotificationTargetManager.runStatusToTaskStatus(RunStatus.NotRan).isEmpty)
    }
  }
}
