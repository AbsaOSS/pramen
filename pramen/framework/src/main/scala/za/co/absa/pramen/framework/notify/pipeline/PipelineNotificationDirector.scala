/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.notify.pipeline

import com.typesafe.config.Config
import za.co.absa.pramen.framework.config.Keys
import za.co.absa.pramen.framework.config.WatcherConfig.{DRY_RUN, RUN_UNDERCOVER}
import za.co.absa.pramen.framework.utils.ConfigUtils

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
    val undercover = ConfigUtils.getOptionBoolean(conf, RUN_UNDERCOVER).getOrElse(false)

    notificationBuilder.addAppName(notification.pipelineName)
    notificationBuilder.addEnvironmentName(notification.environmentName)
    notificationBuilder.addAppDuration(notification.started, notification.finished)
    notificationBuilder.addDryRun(dryRun)
    notificationBuilder.addUndercover(undercover)

    notification.exception.foreach(notificationBuilder.addFailureException)

    notificationBuilder.addRpsMetrics(minRps, goodRps)

    notification
      .tasksCompleted
      .foreach(notificationBuilder.addCompletedTask)

    notificationBuilder
  }

}
