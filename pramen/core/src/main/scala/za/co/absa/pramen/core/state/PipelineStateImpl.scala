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

package za.co.absa.pramen.core.state

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.Pramen
import za.co.absa.pramen.core.app.config.HookConfig
import za.co.absa.pramen.core.app.config.RuntimeConfig.EMAIL_IF_NO_CHANGES
import za.co.absa.pramen.core.metastore.peristence.MetastorePersistenceTransient
import za.co.absa.pramen.core.notify.pipeline.{PipelineNotification, PipelineNotificationEmail}
import za.co.absa.pramen.core.pipeline.PipelineDef._
import za.co.absa.pramen.core.runner.task.RunStatus.NotRan
import za.co.absa.pramen.core.runner.task.TaskResult

import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.util.control.NonFatal

class PipelineStateImpl(implicit conf: Config) extends PipelineState {
  private val log = LoggerFactory.getLogger(this.getClass)

  // Config
  private val pipelineName = conf.getString(PIPELINE_NAME_KEY)
  private val environmentName = conf.getString(ENVIRONMENT_NAME)
  private val sendEmailIfNoNewData: Boolean = conf.getBoolean(EMAIL_IF_NO_CHANGES)
  private val hookConfig = HookConfig.fromConfig(conf)

  // State
  private val startedInstant = Instant.now
  private var exitCode = 0
  private val taskResults = new ListBuffer[TaskResult]
  private var failureException: Option[Throwable] = None
  private var exitedNormally = false
  private var isFinished = false

  override def setSuccess(): Unit = synchronized {
    protectAgainstDoubleFinish()
    exitedNormally = true
    runCustomShutdownHook()
    sendNotificationEmail()
  }

  override def setFailure(stage: String, exception: Throwable): Unit = synchronized {
    protectAgainstDoubleFinish()
    if (failureException.isEmpty) {
      failureException = Some(exception)
    }
    exitCode = 1
    exitedNormally = false
    runCustomShutdownHook()
    sendNotificationEmail()
  }

  override def addTaskCompletion(statuses: Seq[TaskResult]): Unit = synchronized {
    taskResults ++= statuses.filter(_.runStatus != NotRan)
    if (statuses.exists(_.runStatus.isFailure)) {
      exitCode = 2
    }
  }

  override def getExitCode: Int = synchronized {
    exitCode
  }

  private def protectAgainstDoubleFinish(): Unit = {
    if (isFinished) {
      throw new IllegalStateException(s"Attempt to run post finish tasks multiple times")
    }
    isFinished = true
    Runtime.getRuntime.removeShutdownHook(shutdownHook)
  }

  private val shutdownHook = new Thread() {
    override def run(): Unit = {
      if (!exitedNormally && !isFinished) {
        if (failureException.isEmpty) {
          failureException = Some(new IllegalStateException("The application exited unexpectedly."))
        }

        runCustomShutdownHook()
        sendNotificationEmail()
        Try{
          // Clean up transient metastore tables if any
          MetastorePersistenceTransient.cleanup()
        }.recover {
          case NonFatal(ex) => log.error("Unable to clean up transient metastore tables.", ex)
        }
      }
    }
  }

  Runtime.getRuntime.addShutdownHook(shutdownHook)

  private def runCustomShutdownHook(): Unit = {
    try {
      hookConfig.shutdownHook.foreach(_.run())
    } catch {
      case NonFatal(ex) => log.error(s"Unable to run a shutdown hook.", ex)
    }
  }

  private def sendNotificationEmail(): Unit = {
    failureException.foreach(ex => log.error(s"The job has FAILED.", ex))

    try {
      val realTaskResults = if (taskResults.exists(!_.isTransient)) {
        taskResults
      } else {
        taskResults.filterNot(_.isTransient)
      }
      val finishedInstant = Instant.now
      val notificationBuilder = Pramen.instance.notificationBuilder.asInstanceOf[NotificationBuilderImpl]
      val customEntries = notificationBuilder.entries
      val customSignature = notificationBuilder.signature

      val notification = PipelineNotification(failureException,
        pipelineName,
        environmentName,
        startedInstant,
        finishedInstant,
        realTaskResults.toList,
        customEntries.toList,
        customSignature.toList)
      if (realTaskResults.nonEmpty || sendEmailIfNoNewData || failureException.nonEmpty) {
        val email = new PipelineNotificationEmail(notification)
        email.send()
      } else {
        log.info("No tasks were ran. The empty notification email won't be sent.")
      }
    } catch {
      case NonFatal(ex) => log.error(s"Unable to send an email notification.", ex)
    }
  }
}
