package za.co.absa.pramen.core.notify

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api.{NotificationTarget, TaskStatus}
import za.co.absa.pramen.core.ExternalChannelFactoryReflect
import za.co.absa.pramen.core.runner.task.RunStatus

object NotificationTargetManager {
  val NOTIFICATION_TARGETS_KEY = "pramen.notification.targets"

  /**
    * Returns a notification target by its name.
    *
    * Example:
    * {{{
    *   pramen.notification.targets = [
    *     {
    *       name = "hyperdrive1"
    *       factory.class = "za.co.absa.pramen.core.notify.HyperdriveNotificationTarget"
    *
    *       kafka.topic = "mytopic"
    *
    *       kafka.option {
    *         bootstrap.servers = "dummy:9092,dummy:9093"
    *         sasl.mechanism = "GSSAPI"
    *         security.protocol = "SASL_SSL"
    *       }
    *    }
    * ]
    * }}}
    *
    * Here 'hyperdrive1' is the name of the notification target.
    *
    * @param name         The name of the notification target.
    * @param conf         The configuration of the application.
    * @param overrideConf A configuration containing override keys for the notification target.
    *
    */
  def getByName(name: String,
                conf: Config,
                overrideConf: Option[Config])
               (implicit spark: SparkSession): NotificationTarget = {
    ExternalChannelFactoryReflect.fromConfigByName[NotificationTarget](conf, overrideConf, NOTIFICATION_TARGETS_KEY, name, "notification target")
  }

  /**
    * Converts an internal run status to the API trait TaskStatus.
    *
    * Converts only statuses which corresponds to jobs actually attempted to run.
    * Returns None otherwise.
    *
    * @param status The internal run status.
    * @return The corresponding task status if applicable.
    */
  def runStatusToTaskStatus(status: RunStatus): Option[TaskStatus] = {
    status match {
      case s: RunStatus.Succeeded => Some(TaskStatus.Succeeded(s.recordCount))
      case s: RunStatus.Failed => Some(TaskStatus.Failed(s.ex))
      case s: RunStatus.ValidationFailed => Some(TaskStatus.ValidationFailed(s.ex))
      case s: RunStatus.MissingDependencies => Some(TaskStatus.MissingDependencies(s.tables))
      case s: RunStatus.FailedDependencies => Some(TaskStatus.FailedDependencies(s.failures.flatMap(d => d.failedTables ++ d.emptyTables).distinct.sorted))
      case RunStatus.NoData => Some(TaskStatus.NoData)
      case s: RunStatus.InsufficientData => Some(TaskStatus.InsufficientData(s.actual, s.expected))
      case s: RunStatus.Skipped => Some(TaskStatus.Skipped(s.msg))
      case _ => None
    }
  }
}
