package za.co.absa.pramen.core.notify

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api.{NotificationTarget, TaskStatus}
import za.co.absa.pramen.core.ExternalChannelFactoryReflect
import za.co.absa.pramen.core.runner.task.RunStatus
import za.co.absa.pramen.core.utils.ConfigUtils

object NotificationTargetManager {
  val NOTIFICATION_TARGETS_KEY = "pramen.notification.targets"

  def getByName(name: String,
                conf: Config,
                overrideConf: Option[Config])
               (implicit spark: SparkSession): NotificationTarget = {
    ExternalChannelFactoryReflect.fromConfigByName[NotificationTarget](conf, overrideConf, NOTIFICATION_TARGETS_KEY, name, "notification target")
  }

  def runStatusToTaskStatus(status: RunStatus): TaskStatus = {
    status match {
      case s: RunStatus.Succeeded => TaskStatus.Succeeded(s.recordCount)
      case s: RunStatus.Failed => TaskStatus.Failed(s.ex)
      case s: RunStatus.ValidationFailed => TaskStatus.ValidationFailed(s.ex)
      case s: RunStatus.MissingDependencies => TaskStatus.MissingDependencies(s.tables)
      case s: RunStatus.FailedDependencies => TaskStatus.FailedDependencies(s.failures.flatMap(_.failedTables).distinct.sorted)
      case RunStatus.NoData => TaskStatus.NoData
      case s: RunStatus.InsufficientData => TaskStatus.InsufficientData(s.actual, s.expected)
      case s: RunStatus.Skipped => TaskStatus.Skipped(s.msg)
    }
  }

  def getNotificationOptions(jobConf: Config): Map[String, String] = {
    ConfigUtils.getExtraOptions(jobConf, "notification")
  }
}
