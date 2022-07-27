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

package za.co.absa.pramen.core.mocks.notify

import za.co.absa.pramen.core.notify.pipeline.PipelineNotificationBuilder
import za.co.absa.pramen.core.runner.task.TaskResult

import java.time.Instant

class PipelineNotificationBuilderSpy extends PipelineNotificationBuilder {
  var failureException: Option[Throwable] = None
  var appName = ""
  var environmentName = ""
  var appStarted: Instant = Instant.MIN
  var appFinished: Instant = Instant.MIN
  var isDryRun: Option[Boolean] = None
  var isUndercover: Option[Boolean] = None
  var minRps = 0
  var goodRps = 0

  var addCompletedTaskCalled = 0

  override def addFailureException(ex: Throwable): Unit = failureException = Option(ex)

  override def addAppName(name: String): Unit = appName = name

  override def addEnvironmentName(env: String): Unit = environmentName = env

  override def addAppDuration(started: Instant, finished: Instant): Unit = {
    appStarted = started
    appFinished = finished
  }

  override def addDryRun(dryRun: Boolean): Unit = isDryRun = Some(dryRun)

  override def addUndercover(undercover: Boolean): Unit = isUndercover = Some(undercover)

  override def addRpsMetrics(min: Int, good: Int): Unit = {
    minRps = min
    goodRps = good
  }

  def addCompletedTask(completedTask: TaskResult): Unit = addCompletedTaskCalled += 1
}
