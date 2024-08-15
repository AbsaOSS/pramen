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

import za.co.absa.pramen.api.notification.{NotificationEntry, TextElement}
import za.co.absa.pramen.api.status.{PipelineNotificationFailure, TaskResult}

import java.time.Instant

trait PipelineNotificationBuilder {
  def addFailureException(ex: Throwable): Unit

  def addWarningFlag(warningFlag: Boolean): Unit

  def addAppName(appName: String): Unit

  def addSparkAppId(sparkAppId: String): Unit

  def addEnvironmentName(env: String): Unit

  def addAppDuration(appStarted: Instant, appFinished: Instant): Unit

  def addDryRun(isDryRun: Boolean): Unit

  def addUndercover(isUndercover: Boolean): Unit

  def addRpsMetrics(minRps: Int, goodRps: Int): Unit

  def addCompletedTask(completedTask: TaskResult): Unit

  def addPipelineNotificationFailure(failure: PipelineNotificationFailure): Unit

  def addCustomEntries(entries: Seq[NotificationEntry]): Unit

  def addSignature(signature: TextElement*): Unit
}
