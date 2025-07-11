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
import sun.misc.Signal
import za.co.absa.pramen.api.status.RunStatus.{NotRan, Succeeded}
import za.co.absa.pramen.api.status._
import za.co.absa.pramen.api.{NotificationBuilder, PipelineInfo, PipelineNotificationTarget}
import za.co.absa.pramen.core.app.config.RuntimeConfig.{DRY_RUN, EMAIL_IF_NO_CHANGES, UNDERCOVER}
import za.co.absa.pramen.core.app.config.{HookConfig, RuntimeConfig}
import za.co.absa.pramen.core.config.Keys.{GOOD_THROUGHPUT_RPS, WARN_THROUGHPUT_RPS}
import za.co.absa.pramen.core.metastore.peristence.{TransientJobManager, TransientTableManager}
import za.co.absa.pramen.core.notify.PipelineNotificationTargetFactory
import za.co.absa.pramen.core.notify.pipeline.{PipelineNotification, PipelineNotificationEmail}
import za.co.absa.pramen.core.pipeline.PipelineDef._
import za.co.absa.pramen.core.utils.{ConfigUtils, JvmUtils}

import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class PipelineStateImpl(implicit conf: Config, notificationBuilder: NotificationBuilder) extends PipelineState {
  import PipelineStateImpl._

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  // Config
  private val runtimeConfig = RuntimeConfig.fromConfig(conf)
  private val pipelineId = java.util.UUID.randomUUID.toString
  private val pipelineName = conf.getString(PIPELINE_NAME_KEY)
  private val environmentName = conf.getString(ENVIRONMENT_NAME)
  private val tenant = ConfigUtils.getOptionString(conf, TENANT_KEY)
  private val sendEmailIfNoNewData: Boolean = conf.getBoolean(EMAIL_IF_NO_CHANGES)
  private val hookConfig = HookConfig.fromConfig(conf)
  private var pipelineNotificationTargets: Seq[PipelineNotificationTarget] = Seq.empty
  private val batchId = Instant.now().toEpochMilli
  private val strictFailures = ConfigUtils.getOptionBoolean(conf, NOTIFICATION_STRICT_FAILURES_KEY).getOrElse(true)

  // State
  private val startedInstant = Instant.now
  private var finishedInstant: Option[Instant] = None
  @volatile private var exitCode = EXIT_CODE_SUCCESS
  private val taskResults = new ListBuffer[TaskResult]
  private val pipelineNotificationFailures = new ListBuffer[PipelineNotificationFailure]
  private val signalHandlers = new ListBuffer[PramenSignalHandler]
  @volatile private var failureException: Option[Throwable] = None
  @volatile private var signalException: Option[Throwable] = None
  @volatile private var exitedNormally = false
  @volatile private var isFinished = false
  @volatile private var customShutdownHookCanRun = false
  @volatile private var sparkAppId: Option[String] = None
  @volatile private var warningFlag: Boolean = false

  init()

  private def init(): Unit = {
    Runtime.getRuntime.addShutdownHook(shutdownHook)

    setSignalHandler(new Signal("INT"), "SIGINT (Ctrl + C)")
    setSignalHandler(new Signal("TERM"), "SIGTERM (kill)")
    setSignalHandler(new Signal("HUP"), "SIGHUP (network connection to the terminal has been lost)")
  }

  private[core] def initNotificationTargets(): Unit = {
    pipelineNotificationTargets = PipelineNotificationTargetFactory.fromConfig(conf)
  }

  override def getState: PipelineStateSnapshot = synchronized {
    val notificationBuilderImpl = notificationBuilder.asInstanceOf[NotificationBuilderImpl]
    val customNotification = CustomNotification (
      notificationBuilderImpl.entries,
      notificationBuilderImpl.signature
    )

    PipelineStateSnapshot(
      getPipelineInfo,
      batchId,
      isFinished,
      exitedNormally,
      exitCode,
      customShutdownHookCanRun,
      taskResults.toList,
      pipelineNotificationFailures.toList,
      customNotification
    )
  }

  def getPipelineInfo: PipelineInfo = synchronized {
    val appException = if (!exitedNormally && failureException.isEmpty && signalException.isDefined) {
      signalException
    } else
      failureException

    val minRps = ConfigUtils.getOptionInt(conf, WARN_THROUGHPUT_RPS).getOrElse(0)
    val goodRps = ConfigUtils.getOptionInt(conf, GOOD_THROUGHPUT_RPS).getOrElse(0)
    val dryRun = ConfigUtils.getOptionBoolean(conf, DRY_RUN).getOrElse(false)
    val undercover = ConfigUtils.getOptionBoolean(conf, UNDERCOVER).getOrElse(false)
    val pipelineStatus = PipelineStateImpl.pipelineStatus(appException, taskResults.toSeq, pipelineNotificationFailures.toSeq, warningFlag, strictFailures)

    PipelineInfo(
      pipelineName,
      environmentName,
      RuntimeInfo(
        runtimeConfig.runDate,
        runtimeConfig.runDateTo,
        runtimeConfig.runDateTo.map(_ => runtimeConfig.historicalRunMode),
        runtimeConfig.isRerun,
        dryRun,
        undercover,
        runtimeConfig.checkOnlyNewData,
        runtimeConfig.checkOnlyLateData,
        minRps,
        goodRps
      ),
      startedInstant,
      finishedInstant,
      warningFlag,
      sparkAppId,
      pipelineStatus,
      appException,
      pipelineNotificationFailures.toSeq,
      pipelineId,
      tenant
    )
  }

  override def getBatchId: Long = batchId

  override def setShutdownHookCanRun(): Unit = synchronized {
    customShutdownHookCanRun = true
  }

  override def setSuccess(): Unit = synchronized {
    if (!alreadyFinished()) {
      exitedNormally = true
      onAppFinish()
    }
  }

  override def setWarningFlag(): Unit = synchronized {
    warningFlag = true
  }

  override def setFailure(stage: String, exception: Throwable): Unit = synchronized {
    if (!alreadyFinished()) {
      setFailureException(exception)
      exitCode |= EXIT_CODE_APP_FAILED
      exitedNormally = false
      onAppFinish()
    }
  }

  override def setSparkAppId(sparkAppId: String): Unit = synchronized {
    this.sparkAppId = Option(sparkAppId)
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

  private[state] def alreadyFinished(): Boolean = {
    if (!isFinished) {
      isFinished = true
      close()
      false
    } else {
      true
    }
  }

  private[state] def onAppFinish(): Unit = {
    if (!exitedNormally && failureException.isEmpty && signalException.isDefined) {
      failureException = signalException
      exitCode |= EXIT_CODE_SIGNAL_RECEIVED
    }

    finishedInstant = Option(Instant.now())
    sendPipelineNotifications()
    runCustomShutdownHook()
    removeSignalHandlers()
    sendNotificationEmail()
  }

  private lazy val shutdownHook = new Thread() {
    override def run(): Unit = {
      if (!exitedNormally && !isFinished) {
        if (failureException.isEmpty && signalException.isEmpty) {
          setFailureException(new IllegalStateException("The application exited unexpectedly."))

          val nonDaemonStackTraces = JvmUtils.getStackTraces
          val renderedStackTraces = JvmUtils.renderStackTraces(nonDaemonStackTraces)

          log.error("Stack traces at the moment of the unexpected exit:\n" + renderedStackTraces)
        }

        isFinished = true

        onAppFinish()
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

  private[state] def sendPipelineNotifications(): Unit = {
    pipelineNotificationTargets.foreach(notificationTarget => sendCustomNotification(notificationTarget, getState, taskResults.toSeq))
  }

  private[state] def sendCustomNotification(pipelineNotificationTarget: PipelineNotificationTarget, pipelineStateSnapshot: PipelineStateSnapshot, taskResults: Seq[TaskResult]): Unit = {
    try {
      pipelineNotificationTarget.sendNotification(
        pipelineStateSnapshot.pipelineInfo,
        taskResults.toSeq,
        pipelineStateSnapshot.customNotification
      )
    } catch {
      case ex: Throwable =>
        log.error(s"Unable to send a notification to the custom notification target: ${pipelineNotificationTarget.getClass.getName}", ex)
        pipelineNotificationFailures += PipelineNotificationFailure(pipelineNotificationTarget.getClass.getName, ex)
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
      val notificationBuilderImpl = notificationBuilder.asInstanceOf[NotificationBuilderImpl]
      val customEntries = notificationBuilderImpl.entries
      val customSignature = notificationBuilderImpl.signature

      val pipelineInfo = getPipelineInfo
      val finishedAt = pipelineInfo.finishedAt.getOrElse(Instant.now())
      val finishedPipelineInfo = pipelineInfo.copy(finishedAt = Option(finishedAt))

      val notification = PipelineNotification(
        finishedPipelineInfo,
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

  private def removeSignalHandlers(): Unit = {
    signalHandlers.foreach(handler => handler.unhandle())
    signalHandlers.clear()
  }

  override def close(): Unit = {
    JvmUtils.safeRemoveShutdownHook(shutdownHook)
  }

  private def setSignalHandler(signal: Signal, signalName: String): Unit = {
    val newHandler = new PramenSignalHandler(signal, signalName, this)
    val oldHandler = Signal.handle(signal, newHandler)
    newHandler.setOldSignalHandler(oldHandler)
    signalHandlers.append(newHandler)
  }

  private[state] def setFailureException(ex: Throwable): Unit = {
    if (failureException.isEmpty) {
      failureException = Option(ex)
    }
  }

  private[state] def getFailureException: Option[Throwable] = failureException

  private[state] def setSignalException(ex: Throwable): Unit = {
    if (signalException.isEmpty) {
      signalException = Option(ex)
    }
  }

  private[state] def getSignalException: Option[Throwable] = signalException
}

object PipelineStateImpl {
  val NOTIFICATION_STRICT_FAILURES_KEY = "pramen.notifications.strict.failures"
  val SUPPRESS_WARNING_STARTING_WITH = "Based on outdated tables: "

  val EXIT_CODE_SUCCESS = 0
  val EXIT_CODE_APP_FAILED = 1
  val EXIT_CODE_JOB_FAILED = 2
  val EXIT_CODE_SIGNAL_RECEIVED = 4

  def pipelineStatus(appException: Option[Throwable], taskResults: Seq[TaskResult], pipelineNotificationFailures: Seq[PipelineNotificationFailure], warningFlag: Boolean, strictFailures: Boolean): PipelineStatus = {
    val isCertainFailure = appException.nonEmpty
    val (someTasksSucceeded, someTasksFailed) = getSuccessFlags(appException, taskResults)

    val warningState = warningFlag || hasWarnings(taskResults, pipelineNotificationFailures)

    if (isCertainFailure) {
      PipelineStatus.Failure
    } else if (!someTasksFailed && !warningState) {
      PipelineStatus.Success
    } else if (someTasksSucceeded && someTasksFailed && !strictFailures) {
      PipelineStatus.PartialSuccess
    } else if (someTasksSucceeded && !someTasksFailed && warningState) {
      PipelineStatus.Warning
    } else {
      PipelineStatus.Failure
    }
  }

  private def getSuccessFlags(appException: Option[Throwable], taskResults: Seq[TaskResult]): (Boolean, Boolean) = {
    val hasNotificationFailures = taskResults.exists(t => t.notificationTargetErrors.nonEmpty)
    val someTasksSucceeded = taskResults.exists(_.runStatus.isInstanceOf[Succeeded]) && appException.isEmpty
    val someTasksFailed = taskResults.exists(t => t.runStatus.isFailure) || hasNotificationFailures || appException.nonEmpty
    (someTasksSucceeded, someTasksFailed)
  }

  private def hasWarnings(taskResults: Seq[TaskResult], pipelineNotificationFailures: Seq[PipelineNotificationFailure]): Boolean = {
    taskResults.exists{task =>
      val hasTaskWarnings = task.runStatus match {
        case succeeded: Succeeded =>
          val warnings = succeeded.warnings
            .filterNot(_.startsWith(SUPPRESS_WARNING_STARTING_WITH))
          warnings.nonEmpty
        case _ =>
          false
      }

      val hasDependencyWarnings = task.dependencyWarnings.nonEmpty
      val hasNotificationErrors = task.notificationTargetErrors.nonEmpty
      val hasSkippedWithWarnings = task.runStatus.isInstanceOf[RunStatus.Skipped] && task.runStatus.asInstanceOf[RunStatus.Skipped].isWarning
      val hasSchemaChanges = task.schemaChanges.nonEmpty
      val hasPipelineNotificationFailures = pipelineNotificationFailures.nonEmpty

      hasDependencyWarnings || hasNotificationErrors || hasTaskWarnings || hasSkippedWithWarnings || hasSchemaChanges || hasPipelineNotificationFailures
    }
  }
}
