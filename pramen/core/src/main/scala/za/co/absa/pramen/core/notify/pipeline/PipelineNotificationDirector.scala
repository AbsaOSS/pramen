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
import za.co.absa.pramen.core.app.config.RuntimeConfig.{DRY_RUN, UNDERCOVER}
import za.co.absa.pramen.core.config.Keys
import za.co.absa.pramen.core.utils.ConfigUtils

object PipelineNotificationDirector {

  /**
    * Apply the builder steps in order to create a formatted notification.
    */
  def build(notificationBuilder: PipelineNotificationBuilder,
            notification: PipelineNotification)
           (implicit conf: Config): PipelineNotificationBuilder = {
    val minRps = conf.getInt(Keys.WARN_THROUGHPUT_RPS)
    val goodRps = conf.getInt(Keys.GOOD_THROUGHPUT_RPS)
    val dryRun = conf.getBoolean(DRY_RUN)
    val undercover = ConfigUtils.getOptionBoolean(conf, UNDERCOVER).getOrElse(false)

    notificationBuilder.addAppName(notification.pipelineName)
    notificationBuilder.addEnvironmentName(notification.environmentName)
    notification.sparkAppId.foreach(id => notificationBuilder.addSparkAppId(id))
    notificationBuilder.addAppDuration(notification.started, notification.finished)
    notificationBuilder.addDryRun(dryRun)
    notificationBuilder.addUndercover(undercover)

    notification.exception.foreach(notificationBuilder.addFailureException)
    notificationBuilder.addWarningFlag(notification.warningFlag)

    notificationBuilder.addRpsMetrics(minRps, goodRps)

    notification
      .tasksCompleted
      .foreach(notificationBuilder.addCompletedTask)

    notification.pipelineNotificationFailures.foreach(pipelineNotificationFailure =>
      notificationBuilder.addPipelineNotificationFailure(pipelineNotificationFailure)
    )

    notificationBuilder.addCustomEntries(notification.customEntries)
    notificationBuilder.addSignature(notification.customSignature: _*)

    notificationBuilder
  }

}
