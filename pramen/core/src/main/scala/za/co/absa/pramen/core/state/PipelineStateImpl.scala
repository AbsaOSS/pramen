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
import org.slf4j.{Logger, LoggerFactory}
import sun.misc.{Signal, SignalHandler}
import za.co.absa.pramen.api.NotificationBuilder
import za.co.absa.pramen.core.app.config.HookConfig
import za.co.absa.pramen.core.app.config.RuntimeConfig.EMAIL_IF_NO_CHANGES
import za.co.absa.pramen.core.exceptions.{OsSignalException, ThreadStackTrace}
import za.co.absa.pramen.core.metastore.peristence.{TransientJobManager, TransientTableManager}
import za.co.absa.pramen.core.notify.pipeline.{PipelineNotification, PipelineNotificationEmail}
import za.co.absa.pramen.core.pipeline.PipelineDef._
import za.co.absa.pramen.core.runner.task.RunStatus.NotRan
import za.co.absa.pramen.core.runner.task.TaskResult

import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

class PipelineStateImpl(implicit conf: Config, notificationBuilder: NotificationBuilder) extends PipelineState {
  import PipelineStateImpl._

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  // Config
  private val pipelineName = conf.getString(PIPELINE_NAME_KEY)
  private val environmentName = conf.getString(ENVIRONMENT_NAME)
  private val sendEmailIfNoNewData: Boolean = conf.getBoolean(EMAIL_IF_NO_CHANGES)
  private val hookConfig = HookConfig.fromConfig(conf)

  // State
  private val startedInstant = Instant.now
  @volatile private var exitCode = EXIT_CODE_SUCCESS
  private val taskResults = new ListBuffer[TaskResult]
  @volatile private var failureException: Option[Throwable] = None
  @volatile private var exitedNormally = false
  @volatile private var isFinished = false
  @volatile private var customShutdownHookCanRun = false

  init()

  private def init(): Unit = {
    Runtime.getRuntime.addShutdownHook(shutdownHook)
    Signal.handle(new Signal("INT"), getSignalHandler("SIGINT (Ctrl + C)"))
    Signal.handle(new Signal("TERM"), getSignalHandler("SIGTERM (kill)"))
    Signal.handle(new Signal("HUP"), getSignalHandler("SIGHUP (network connection to the terminal has been lost)"))
    Signal.handle(new Signal("PIPE"), getSignalHandler("SIGPIPE (attempt to write to a pipe that is no longer available)"))
  }

  override def getState(): PipelineStateSnapshot = synchronized {
    PipelineStateSnapshot(
      isFinished,
      exitedNormally,
      exitCode,
      customShutdownHookCanRun,
      failureException,
      taskResults.toList
    )
  }

  override def setShutdownHookCanRun(): Unit = synchronized {
    customShutdownHookCanRun = true
  }

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
    exitCode |= EXIT_CODE_APP_FAILED
    exitedNormally = false
    runCustomShutdownHook()
    sendNotificationEmail()
  }

  override def addTaskCompletion(statuses: Seq[TaskResult]): Unit = synchronized {
    taskResults ++= statuses.filter(_.runStatus != NotRan)
    if (statuses.exists(_.runStatus.isFailure)) {
      exitCode |= EXIT_CODE_JOB_FAILED
    }
  }

  override def getExitCode: Int = synchronized {
    exitCode
  }

  private[state] def protectAgainstDoubleFinish(): Unit = {
    if (isFinished) {
      throw new IllegalStateException(s"Attempt to run post finish tasks multiple times")
    }
    isFinished = true
    close()
  }

  private lazy val shutdownHook = new Thread() {
    override def run(): Unit = {
      if (!exitedNormally && !isFinished) {
        if (failureException.isEmpty) {
          failureException = Some(new IllegalStateException("The application exited unexpectedly."))
        }

        runCustomShutdownHook()
        sendNotificationEmail()
        Try {
          // Clean up transient metastore state if any
          TransientTableManager.reset()
          TransientJobManager.reset()
        }.recover {
          case NonFatal(ex) => log.error("Unable to clean up transient metastore tables.", ex)
        }
      }
    }
  }

  private[state] def runCustomShutdownHook(): Unit = {
    if (customShutdownHookCanRun) {
      try {
        hookConfig.shutdownHook.foreach {
          case Success(runnable) =>
            runnable.run()
          case Failure(ex) =>
            // The error should already be thrown at the validation stage. This is just a precaution.
            throw ex
        }
      } catch {
        case ex: Throwable =>
          log.error(s"Unable to run the shutdown hook.", ex)
          if (failureException.isEmpty) {
            setFailure("running the shutdown hook", ex)
          }
      }
    }
  }

  protected def sendNotificationEmail(): Unit = {
    failureException.foreach(ex => log.error(s"The job has FAILED.", ex))

    try {
      val realTaskResults = if (taskResults.exists(!_.isTransient)) {
        taskResults
      } else {
        taskResults.filterNot(_.isTransient)
      }
      val finishedInstant = Instant.now
      val notificationBuilderImpl = notificationBuilder.asInstanceOf[NotificationBuilderImpl]
      val customEntries = notificationBuilderImpl.entries
      val customSignature = notificationBuilderImpl.signature

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

  override def close(): Unit = Runtime.getRuntime.removeShutdownHook(shutdownHook)

  private def getSignalHandler(signalName: String): SignalHandler = new SignalHandler {
    override def handle(sig: Signal): Unit = {
      val stackTraces = Thread.getAllStackTraces.asScala

      val nonDaemonStackTraces = stackTraces.flatMap{ case (t: Thread, s: Array[StackTraceElement]) =>
        if (t.isDaemon) {
          None
        } else {
          Option(ThreadStackTrace(t.getName, s))
        }
      }.toSeq

      val ex = OsSignalException(signalName, nonDaemonStackTraces)
      if (failureException.isEmpty) {
        failureException = Some(ex)
      }

      exitCode |= EXIT_CODE_SIGNAL_RECEIVED
      System.exit(exitCode)
    }
  }
}

object PipelineStateImpl {
  val EXIT_CODE_SUCCESS = 0
  val EXIT_CODE_APP_FAILED = 1
  val EXIT_CODE_JOB_FAILED = 2
  val EXIT_CODE_SIGNAL_RECEIVED = 4
}
