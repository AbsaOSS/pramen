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
import za.co.absa.pramen.api.status.RunStatus.NotRan
import za.co.absa.pramen.api.status._
import za.co.absa.pramen.api.{NotificationBuilder, PipelineInfo, PipelineNotificationTarget}
import za.co.absa.pramen.core.app.config.HookConfig
import za.co.absa.pramen.core.app.config.RuntimeConfig.{DRY_RUN, EMAIL_IF_NO_CHANGES, UNDERCOVER}
import za.co.absa.pramen.core.config.Keys.{GOOD_THROUGHPUT_RPS, WARN_THROUGHPUT_RPS}
import za.co.absa.pramen.core.metastore.peristence.{TransientJobManager, TransientTableManager}
import za.co.absa.pramen.core.notify.PipelineNotificationTargetFactory
import za.co.absa.pramen.core.notify.pipeline.{PipelineNotification, PipelineNotificationEmail}
import za.co.absa.pramen.core.pipeline.PipelineDef._
import za.co.absa.pramen.core.utils.ConfigUtils

import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class PipelineStateImpl(implicit conf: Config, notificationBuilder: NotificationBuilder) extends PipelineState {
  import PipelineStateImpl._

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  // Config
  private val pipelineId = java.util.UUID.randomUUID.toString
  private val pipelineName = conf.getString(PIPELINE_NAME_KEY)
  private val environmentName = conf.getString(ENVIRONMENT_NAME)
  private val tenant = ConfigUtils.getOptionString(conf, TENANT_KEY)
  private val sendEmailIfNoNewData: Boolean = conf.getBoolean(EMAIL_IF_NO_CHANGES)
  private val hookConfig = HookConfig.fromConfig(conf)
  private var pipelineNotificationTargets: Seq[PipelineNotificationTarget] = Seq.empty

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

  override def getState(): PipelineStateSnapshot = synchronized {
    val appException =  if (!exitedNormally && failureException.isEmpty && signalException.isDefined) {
      signalException
    } else
      failureException

    val notificationBuilderImpl = notificationBuilder.asInstanceOf[NotificationBuilderImpl]
    val customNotification = CustomNotification (
      notificationBuilderImpl.entries,
      notificationBuilderImpl.signature
    )

    val minRps = ConfigUtils.getOptionInt(conf, WARN_THROUGHPUT_RPS).getOrElse(0)
    val goodRps = ConfigUtils.getOptionInt(conf, GOOD_THROUGHPUT_RPS).getOrElse(0)
    val dryRun = ConfigUtils.getOptionBoolean(conf, DRY_RUN).getOrElse(false)
    val undercover = ConfigUtils.getOptionBoolean(conf, UNDERCOVER).getOrElse(false)

    PipelineStateSnapshot(
      PipelineInfo(
        pipelineName,
        environmentName,
        RuntimeInfo(
          dryRun,
          undercover,
          minRps,
          goodRps
        ),
        startedInstant,
        finishedInstant,
        sparkAppId,
        appException,
        pipelineNotificationFailures.toSeq,
        pipelineId,
        tenant
      ),
      isFinished,
      warningFlag,
      exitedNormally,
      exitCode,
      customShutdownHookCanRun,
      taskResults.toList,
      pipelineNotificationFailures.toList,
      customNotification
    )
  }

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
        if (failureException.isEmpty && signalException.isEmpty)
          setFailureException(new IllegalStateException("The application exited unexpectedly."))

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
    pipelineNotificationTargets.foreach(notificationTarget => sendCustomNotification(notificationTarget, getState(), taskResults.toSeq))
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

      val notification = PipelineNotification(failureException,
        warningFlag,
        pipelineName,
        environmentName,
        sparkAppId,
        startedInstant,
        finishedInstant.getOrElse(Instant.now()),
        realTaskResults.toList,
        pipelineNotificationFailures.toList,
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
    Try {
      // Ignore runtime exceptions, including "java.lang.IllegalStateException: Shutdown in progress"
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
    }
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
  val EXIT_CODE_SUCCESS = 0
  val EXIT_CODE_APP_FAILED = 1
  val EXIT_CODE_JOB_FAILED = 2
  val EXIT_CODE_SIGNAL_RECEIVED = 4
}
