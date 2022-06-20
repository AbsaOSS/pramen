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

package za.co.absa.pramen.framework.state

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import za.co.absa.pramen.framework.config.WatcherConfig
import za.co.absa.pramen.framework.config.WatcherConfig.NON_ZERO_EXIT_CODE_IF_NO_DATA
import za.co.absa.pramen.framework.journal.Journal
import za.co.absa.pramen.framework.notify.{Notification, SchemaDifference, SyncNotificationEmail, TaskCompleted}
import za.co.absa.pramen.framework.utils.ConfigUtils

import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

/**
  * This class provides facilities to
  * - keep track of state of running jobs,
  * - send notification emails on both success and failure of the application.
  *
  * The state is isolated in this class. The usage is like this:
  * {{{
  *   val runState = new RunState
  *   try {
  *     runJobs(...)
  *     runState.setSuccess()
  *   } catch {
  *     case NonFatal(ex) => runState.setFailure(ex)
  *   }
  * }}}
  */
class RunAppState(implicit conf: Config) extends RunState {
  // Dependencies
  private val syncConfig = WatcherConfig.load(conf)
  private val log = LoggerFactory.getLogger(this.getClass)

  // Configuration
  private val ingestionName = syncConfig.ingestionName
  private val environmentName = syncConfig.environmentName

  // State
  private val startedInstant = Instant.now
  private val tasksCompleted = new ListBuffer[TaskCompleted]
  private val schemaDifferences = new ListBuffer[SchemaDifference]
  private var error: Option[Throwable] = None
  private var exitedNormally = false
  private var isFinished = false
  private var journal: Option[Journal] = None
  private var sendEmailIfNoNewData: Boolean = false

  def getCompletedTasks: Seq[TaskCompleted] = tasksCompleted

  private val shutdownHook = new Thread() {
    override def run(): Unit = {
      if (!exitedNormally && !isFinished) {
        error = Option(new IllegalStateException("The application exited unexpectedly."))
        sendNotificationEmail()
      }
    }
  }

  Runtime.getRuntime.addShutdownHook(shutdownHook)

  def setJournal(j: Journal): Unit = journal = Option(j)

  def setSendEmailIfNoData(newValue: Boolean): Unit = sendEmailIfNoNewData = newValue

  /**
    * Invoke this method for each completed task.
    */
  def addCompletedTask(taskCompleted: TaskCompleted): Unit = {
    journal.foreach(_.addEntry(taskCompleted))
    tasksCompleted += taskCompleted
  }

  /**
    * Invoke this method for schema differences for a table.
    */
  def addSchemaDifference(schemaDifference: SchemaDifference): Unit = {
    schemaDifferences += schemaDifference
  }

  /**
    * Invoke this method then all jobs have completed successfully.
    */
  def setSuccess(): Unit = {
    protectAgainstDoubleFinish()
    exitedNormally = true
    sendNotificationEmail()
  }

  /**
    * Invoke this method then the application failed to finish all of the jobs.
    */
  def setFailure(exception: Throwable): Unit = {
    protectAgainstDoubleFinish()
    error = Option(exception)
    exitedNormally = false
    sendNotificationEmail()
  }

  private def sendNotificationEmail(): Unit = {
    error.foreach(ex => log.error(s"The job has FAILED.", ex) )

    try {
      val finishedInstant = Instant.now
      val notification = Notification(error,
        ingestionName,
        environmentName,
        startedInstant,
        finishedInstant,
        tasksCompleted.toList,
        schemaDifferences.toList)
      if (tasksCompleted.nonEmpty || sendEmailIfNoNewData || error.nonEmpty) {
        val email = new SyncNotificationEmail(notification)
        email.send()
      }
    } catch {
      case NonFatal(ex) => log.error(s"Unable to send an email notification.", ex)
    }
  }

  override def getExitCode: Int = {
    val SUCCESS = 0
    val FAILURE = 1

    ConfigUtils.validateOneOfPathsExistence(conf, "", NON_ZERO_EXIT_CODE_IF_NO_DATA :: Nil)
    val isNonZeroExitCodeOnNoData = conf.getBoolean(NON_ZERO_EXIT_CODE_IF_NO_DATA)

    if (exitedNormally) {
      if (isNonZeroExitCodeOnNoData && tasksCompleted.exists(_.failureReason.isDefined)) {
        FAILURE
      } else {
        SUCCESS
      }
    } else {
      FAILURE
    }
  }

  private def protectAgainstDoubleFinish(): Unit = {
    if (isFinished) {
      throw new IllegalStateException(s"Attempt to run post finish tasks multiple times")
    }
    isFinished = true
    Runtime.getRuntime.removeShutdownHook(shutdownHook)
  }

}
