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

package za.co.absa.pramen.core.runner.task

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.status._
import za.co.absa.pramen.core.app.config.RuntimeConfig
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.exceptions.{FatalErrorWrapper, ReasonException}
import za.co.absa.pramen.core.journal.Journal
import za.co.absa.pramen.core.journal.model.TaskCompleted
import za.co.absa.pramen.core.lock.TokenLockFactory
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.pipeline.JobPreRunStatus._
import za.co.absa.pramen.core.pipeline._
import za.co.absa.pramen.core.state.PipelineState
import za.co.absa.pramen.core.utils.Emoji._
import za.co.absa.pramen.core.utils.SparkUtils._
import za.co.absa.pramen.core.utils.{ThreadUtils, TimeUtils}
import za.co.absa.pramen.core.utils.hive.HiveHelper

import java.sql.Date
import java.time.{Instant, LocalDate}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

abstract class TaskRunnerBase(conf: Config,
                              bookkeeper: Bookkeeper,
                              journal: Journal,
                              lockFactory: TokenLockFactory,
                              runtimeConfig: RuntimeConfig,
                              pipelineState: PipelineState,
                              applicationId: String) extends TaskRunner {
  implicit private val ecDefault: ExecutionContext = ExecutionContext.global
  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Runs tasks in parallel (if possible) and returns their futures. Subclasses should override this method.
    *
    * @param tasks Tasks to run.
    * @return A sequence of futures - one for each task.
    */
  def runParallel(tasks: Seq[Task]): Seq[Future[RunStatus]]

  /**
    * Runs tasks only sequentially.
    *
    * @param tasks Tasks to run.
    * @return A sequence of futures - one for each task.
    */
  def runSequential(tasks: Seq[Task]): Future[Seq[RunStatus]]

  override def runJobTasks(job: Job, infoDates: Seq[TaskPreDef]): Future[Seq[RunStatus]] = {
    val tasks = infoDates.map(p => Task(job, p.infoDate, p.reason))

    if (job.allowRunningTasksInParallel) {
      val futures = runParallel(tasks)

      Future.sequence(futures)
    } else {
      runSequential(tasks)
    }
  }

  override def runLazyTask(job: Job, infoDate: LocalDate): RunStatus = {
    val started = Instant.now()
    val task = Task(job, infoDate, TaskRunReason.OnRequest)
    val result: TaskResult = validate(task, started) match {
      case Left(failedResult) => failedResult
      case Right(validationResult) => run(task, started, validationResult)
    }

    onTaskCompletion(task, result, isLazy = true)
  }

  /** Runs multiple tasks in the single thread in the order of info dates. If one task fails, the rest will be skipped. */
  protected def runDependentTasks(tasks: Seq[Task]): Seq[RunStatus] = {
    val sortedTasks = tasks.sortBy(_.infoDate)
    var failedInfoDate: Option[LocalDate] = None

    sortedTasks.map(task =>
      failedInfoDate match {
        case Some(failedDate) =>
          skipTask(task, s"Due to failure for $failedDate", isWarning = true)
        case None =>
          val status = runTask(task)
          if (status.isFailure)
            failedInfoDate = Option(task.infoDate)
          status
      }
    )
  }

  /** Runs a task in the single thread. Performs all task logging and notification sending activities. */
  protected def runTask(task: Task): RunStatus = {
    val started = Instant.now()
    task.job.operation.killMaxExecutionTimeSeconds match {
      case Some(timeout) if timeout > 0 =>
        @volatile var runStatus: RunStatus = null

        try {
          ThreadUtils.runWithTimeout(Duration(timeout, TimeUnit.SECONDS)) {
            log.info(s"Running ${task.job.name} with the hard timeout = $timeout seconds.")
            runStatus = doValidateAndRunTask(task)
          }
          runStatus
        } catch {
          case NonFatal(ex) =>
            failTask(task, started, ex)
        }
      case Some(timeout) =>
        log.error(s"Incorrect timeout for the task: ${task.job.name}. Should be bigger than zero, got: $timeout.")
        doValidateAndRunTask(task)
      case None =>
        doValidateAndRunTask(task)
    }
  }

  protected def doValidateAndRunTask(task: Task): RunStatus = {
    val started = Instant.now()
    task.reason match {
      case TaskRunReason.Skip(reason) =>
        // This skips tasks that were skipped based on strong date constraints (e.g. attempt to run before the minimum date)
        skipTask(task, reason, isWarning = true)
      case _ =>
        val result: TaskResult = validate(task, started) match {
          case Left(failedResult) => failedResult
          case Right(validationResult) => run(task, started, validationResult)
        }
        onTaskCompletion(task, result, isLazy = false)
    }
  }

  /** Skips a task. Performs all task logging and notification sending activities. */
  protected def skipTask(task: Task, reason: String, isWarning: Boolean): RunStatus = {
    val now = Instant.now()
    val runStatus = RunStatus.Skipped(reason, isWarning)
    val runInfo = RunInfo(task.infoDate, now, now)
    val isTransient = task.job.outputTable.format.isTransient
    val taskResult = TaskResult(
      task.job.name,
      MetaTable.getMetaTableDef(task.job.outputTable),
      runStatus,
      Option(runInfo),
      applicationId,
      isTransient,
      isRawFilesJob = false,
      Nil,
      Nil,
      Nil,
      task.job.operation.extraOptions)

    onTaskCompletion(task, taskResult, isLazy = false)
  }

  /** Fails a task. Performs all task logging and notification sending activities. */
  protected def failTask(task: Task, started: Instant, ex: Throwable): RunStatus = {
    val now = Instant.now()
    val runStatus = RunStatus.Failed(ex)
    val runInfo = RunInfo(task.infoDate, started, now)
    val isTransient = task.job.outputTable.format.isTransient
    val isLazy = task.job.outputTable.format.isLazy
    val taskResult = TaskResult(
      task.job.name,
      MetaTable.getMetaTableDef(task.job.outputTable),
      runStatus,
      Option(runInfo),
      applicationId,
      isTransient,
      isRawFilesJob = task.job.outputTable.format.isInstanceOf[DataFormat.Raw],
      Nil,
      Nil,
      Nil,
      task.job.operation.extraOptions)

    onTaskCompletion(task, taskResult, isLazy = isLazy)
  }

  /**
    * Performs a pre-run check. If the check is successful, the job is validated, and then allowed to run.
    *
    * If the check is successful, JobPreRunResult is returned.
    * If the check has failed an instance of TaskResult is returned.
    *
    * @param task    a task to validate.
    * @param started the instant when the job has started executing.
    * @return an instance of TaskResult on the check failure or optional record count on success.
    */
  private[core] def preRunCheck(task: Task, started: Instant): Either[TaskResult, JobPreRunResult] = {
    val jobName = task.job.name
    val outputTable = MetaTable.getMetaTableDef(task.job.outputTable)
    val outputTableName = task.job.outputTable.name
    val options = task.job.operation.extraOptions
    val isTransient = task.job.outputTable.format.isTransient
    val isRawFileBased = task.job.outputTable.format.isInstanceOf[DataFormat.Raw]

    Try {
      task.job.preRunCheck(task.infoDate, task.reason, conf)
    } match {
      case Success(validationResult) =>
        val resultToReturn = validationResult.status match {
          case Ready =>
            log.info(s"Validation of the task: $outputTableName for date: ${task.infoDate} has SUCCEEDED.")
            Right(validationResult)
          case NeedsUpdate =>
            log.info(s"The table needs update: $outputTableName for date: ${task.infoDate}.")
            Right(validationResult)
          case NoData(isFailure) =>
            log.info(s"NO DATA available for the task: $outputTableName for date: ${task.infoDate}.")
            Left(TaskResult(jobName, outputTable, RunStatus.NoData(isFailure), getRunInfo(task.infoDate, started), applicationId, isTransient, isRawFileBased, Nil, validationResult.dependencyWarnings, Nil, options))
          case InsufficientData(actual, expected, oldRecordCount) =>
            log.info(s"INSUFFICIENT DATA available for the task: $outputTableName for date: ${task.infoDate}. Expected = $expected, actual = $actual")
            Left(TaskResult(jobName, outputTable, RunStatus.InsufficientData(actual, expected, oldRecordCount), getRunInfo(task.infoDate, started), applicationId, isTransient, isRawFileBased, Nil, validationResult.dependencyWarnings, Nil, options))
          case AlreadyRan =>
            if (runtimeConfig.isRerun) {
              log.info(s"RE-RUNNING the task: $outputTableName for date: ${task.infoDate}.")
              Right(validationResult)
            } else {
              log.info(s"SKIPPING already ran job: $outputTableName for date: ${task.infoDate}.")
              Left(TaskResult(jobName, outputTable, RunStatus.NotRan, getRunInfo(task.infoDate, started), applicationId, isTransient, isRawFileBased, Nil, validationResult.dependencyWarnings, Nil, options))
            }
          case Skip(msg) =>
            log.info(s"SKIPPING job: $outputTableName for date: ${task.infoDate}. Reason: msg")
            Left(TaskResult(jobName, outputTable, RunStatus.Skipped(msg), getRunInfo(task.infoDate, started), applicationId, isTransient, isRawFileBased, Nil, validationResult.dependencyWarnings, Nil, options))
          case FailedDependencies(isFailure, failures) =>
            Left(TaskResult(jobName, outputTable, RunStatus.FailedDependencies(isFailure, failures), getRunInfo(task.infoDate, started), applicationId, isTransient, isRawFileBased, Nil, Nil, Nil, options))
        }
        if (validationResult.dependencyWarnings.nonEmpty) {
          log.warn(s"$WARNING Validation of the task: $outputTableName for date: ${task.infoDate} has " +
            s"optional dependency failure(s) for table(s): ${validationResult.dependencyWarnings.map(_.table).mkString(", ")} ")
        }
        resultToReturn
      case Failure(ex) =>
        Left(TaskResult(jobName, outputTable, RunStatus.ValidationFailed(ex), getRunInfo(task.infoDate, started), applicationId, isTransient, isRawFileBased, Nil, Nil, Nil, options))
    }
  }

  /**
    * Does pre-run checks and task validations.
    *
    * If validation is successful, in instance of JobPreRunResult is returned.
    * If validation failed an instance of TaskResult is returned.
    *
    * @param task      a task to validate.
    * @param started   the instant when the job has started executing.
    * @return an instance of TaskResult on validation failure or optional record count on success.
    */
  private[core] def validate(task: Task, started: Instant): Either[TaskResult, JobPreRunResult] = {
    val jobName = task.job.name
    val outputTable = MetaTable.getMetaTableDef(task.job.outputTable)
    val outputTableName = task.job.outputTable.name
    val options = task.job.operation.extraOptions
    val isTransient = task.job.outputTable.format.isTransient
    val isRawFileBased = task.job.outputTable.format.isInstanceOf[DataFormat.Raw]

    preRunCheck(task, started) match {
      case Left(result) =>
        Left(result)
      case Right(status) =>
        Try {
          task.job.validate(task.infoDate, conf)
        } match {
          case Success(validationResult) =>
            validationResult match {
              case Reason.Ready =>
                log.info(s"VALIDATION is SUCCESSFUL for the task: $outputTableName for date: ${task.infoDate}.")
                Right(status)
              case reason: Reason.Warning =>
                log.info(s"VALIDATION is SUCCESSFUL with WARNINGS for the task: $outputTableName for date: ${task.infoDate}.")
                Right(status.copy(warnings = reason.warnings))
              case Reason.NotReady(msg) =>
                log.info(s"NOT READY validation failure for the task: $outputTableName for date: ${task.infoDate}. Reason: $msg")
                Left(TaskResult(jobName, outputTable, RunStatus.ValidationFailed(new ReasonException(Reason.NotReady(msg), msg)), getRunInfo(task.infoDate, started), applicationId, isTransient, isRawFileBased, Nil, status.dependencyWarnings, Nil, Map.empty))
              case Reason.Skip(msg) =>
                log.info(s"SKIP validation failure for the task: $outputTableName for date: ${task.infoDate}. Reason: $msg")
                if (bookkeeper.getLatestDataChunk(outputTableName, task.infoDate, task.infoDate).isEmpty) {
                  val isTransient = task.job.outputTable.format.isTransient
                  bookkeeper.setRecordCount(outputTableName, task.infoDate, task.infoDate, task.infoDate, status.inputRecordsCount.getOrElse(0L), 0, started.getEpochSecond, Instant.now().getEpochSecond, isTransient)
                }
                Left(TaskResult(jobName, outputTable, RunStatus.Skipped(msg), getRunInfo(task.infoDate, started), applicationId, isTransient, isRawFileBased, Nil, status.dependencyWarnings, Nil, options))
              case Reason.SkipOnce(msg) =>
                log.info(s"SKIP today validation failure for the task: $outputTableName for date: ${task.infoDate}. Reason: $msg")
                Left(TaskResult(jobName, outputTable, RunStatus.Skipped(msg), getRunInfo(task.infoDate, started), applicationId, isTransient, isRawFileBased, Nil, status.dependencyWarnings, Nil, options))
            }
          case Failure(ex) =>
            Left(TaskResult(jobName, outputTable, RunStatus.ValidationFailed(ex), getRunInfo(task.infoDate, started), applicationId, isTransient, isRawFileBased, Nil, status.dependencyWarnings, Nil, options))
        }
    }
  }

  /**
    * Runs a task.
    *
    * Returns an instance of TaskResult.
    *
    * @param task    a task to run.
    * @param started the instant when the job has started executing.
    * @return an instance of TaskResult.
    */
  private[core] def run(task: Task, started: Instant, validationResult: JobPreRunResult): TaskResult = {
    val isTransient = task.job.outputTable.format.isTransient
    val isRawFileBased = task.job.outputTable.format.isInstanceOf[DataFormat.Raw]
    val lock = lockFactory.getLock(getTokenName(task))

    val attempt = try {
      Try {
        if (!isTransient) {
          if (runtimeConfig.useLocks && !lock.tryAcquire())
            throw new IllegalStateException(s"Another instance is already running for ${task.job.outputTable.name} for ${task.infoDate}")
        }

        val recordCountOldOpt = bookkeeper.getLatestDataChunk(task.job.outputTable.name, task.infoDate, task.infoDate).map(_.outputRecordCount)

        val runResult = task.job.run(task.infoDate, conf)

        val schemaChangesBeforeTransform = handleSchemaChange(runResult.data, task.job.outputTable, task.infoDate)

        val dfWithTimestamp = task.job.operation.processingTimestampColumn match {
          case Some(timestampCol) => addProcessingTimestamp(runResult.data, timestampCol)
          case None => runResult.data
        }

        val dfWithInfoDate = if (dfWithTimestamp.schema.exists(f => f.name.equals(task.job.outputTable.infoDateColumn)) || task.job.outputTable.infoDateColumn.isEmpty) {
          dfWithTimestamp
        } else {
          dfWithTimestamp.withColumn(task.job.outputTable.infoDateColumn, lit(Date.valueOf(task.infoDate)))
        }

        val postProcessed = task.job.postProcessing(dfWithInfoDate, task.infoDate, conf)

        val dfTransformed = applyFilters(
          applyTransformations(postProcessed, task.job.operation.schemaTransformations),
          task.job.operation.filters,
          task.infoDate,
          task.infoDate,
          task.infoDate
        )

        val schemaChangesAfterTransform = if (task.job.operation.schemaTransformations.nonEmpty) {
          val transformedTable = task.job.outputTable.copy(name = s"${task.job.outputTable.name}_transformed")
          handleSchemaChange(dfTransformed, transformedTable, task.infoDate)
        } else {
          Nil
        }

        val saveResult = if (runtimeConfig.isDryRun) {
          log.warn(s"$WARNING DRY RUN mode, no actual writes to ${task.job.outputTable.name} for ${task.infoDate} will be performed.")
          SaveResult(MetaTableStats(dfTransformed.count(), None))
        } else {
          task.job.save(dfTransformed, task.infoDate, conf, started, validationResult.inputRecordsCount)
        }

        val hiveWarnings = if (task.job.outputTable.hiveTable.nonEmpty) {
          val recreate = schemaChangesBeforeTransform.nonEmpty || schemaChangesAfterTransform.nonEmpty || task.reason == TaskRunReason.Rerun
          task.job.createOrRefreshHiveTable(dfTransformed.schema, task.infoDate, recreate)
        } else {
          Seq.empty
        }

        val outputMetastoreHiveTable = task.job.outputTable.hiveTable.map(table => HiveHelper.getFullTable(task.job.outputTable.hiveConfig.database, table))
        val hiveTableUpdates = (saveResult.hiveTablesUpdates ++ outputMetastoreHiveTable).distinct

        val stats = saveResult.stats

        val finished = Instant.now()

        val completionReason = if (validationResult.status == NeedsUpdate || (validationResult.status == AlreadyRan && task.reason != TaskRunReason.Rerun))
          TaskRunReason.Update else task.reason

        val warnings = validationResult.warnings ++ runResult.warnings ++ saveResult.warnings ++ hiveWarnings

        TaskResult(task.job.name,
          MetaTable.getMetaTableDef(task.job.outputTable),
          RunStatus.Succeeded(recordCountOldOpt,
            stats.recordCount,
            stats.dataSizeBytes,
            completionReason,
            runResult.filesRead,
            saveResult.filesSent,
            hiveTableUpdates,
            warnings),
          Some(RunInfo(task.infoDate, started, finished)),
          applicationId,
          isTransient,
          isRawFileBased,
          schemaChangesBeforeTransform ::: schemaChangesAfterTransform,
          validationResult.dependencyWarnings,
          Seq.empty,
          task.job.operation.extraOptions)
      }
    } catch {
      case ex: Throwable => Failure(new FatalErrorWrapper("Fatal error has occurred.", ex))
    } finally {
      if (!isTransient) {
        lock.release()
      }
    }

    attempt match {
      case Success(result) =>
        result
      case Failure(ex) =>
        TaskResult(task.job.name,
          MetaTable.getMetaTableDef(task.job.outputTable),
          RunStatus.Failed(ex),
          getRunInfo(task.infoDate, started),
          applicationId,
          isTransient,
          isRawFileBased,
          Nil,
          validationResult.dependencyWarnings,
          Nil,
          task.job.operation.extraOptions)
    }
  }

  private def getTokenName(task: Task): String = {
    s"${task.job.outputTable.name}_${task.infoDate}"
  }

  /** Logs task completion and sends corresponding notifications. */
  private def onTaskCompletion(task: Task, taskResult: TaskResult, isLazy: Boolean): RunStatus = {
    val notificationTargetErrors = sendNotifications(task, taskResult)
    val updatedResult = taskResult.copy(notificationTargetErrors = notificationTargetErrors)

    logTaskResult(updatedResult, isLazy)
    val wasInterrupted = isTaskInterrupted(task, taskResult)
    if (wasInterrupted) {
      log.warn("Skipping the interrupted exception of the killed task.")
    } else {
      pipelineState.addTaskCompletion(Seq(updatedResult))
      addJournalEntry(task, updatedResult, pipelineState.getState().pipelineInfo)
    }

    updatedResult.runStatus
  }

  private def isTaskInterrupted(task: Task, taskResult: TaskResult): Boolean = {
    val hasTimeout = task.job.operation.killMaxExecutionTimeSeconds.nonEmpty

    taskResult.runStatus match {
      case _: RunStatus.Failed if hasTimeout =>
        val failureException = taskResult.runStatus.asInstanceOf[RunStatus.Failed].ex

        failureException match {
          case _: InterruptedException =>
            true
          case _: FatalErrorWrapper =>
            failureException.getCause match {
              case _: InterruptedException =>
                true
              case _ =>
                false
            }
          case _ =>
            false
        }
      case _ =>
        false
    }
  }

  private def addJournalEntry(task: Task, taskResult: TaskResult, pipelineInfo: PipelineInfo): Unit = {
    val taskCompleted = TaskCompleted.fromTaskResult(task, taskResult, pipelineInfo)

    journal.addEntry(taskCompleted)
  }

  private def sendNotifications(task: Task, result: TaskResult): Seq[NotificationFailure] = {
    val pipelineInfo = pipelineState.getState().pipelineInfo
    task.job.notificationTargets.flatMap(notificationTarget => sendNotifications(task, result, notificationTarget, pipelineInfo))
  }

  private def sendNotifications(task: Task, result: TaskResult, notificationTarget: JobNotificationTarget, pipelineInfo: PipelineInfo): Option[NotificationFailure] = {
    Try {
      val target = notificationTarget.target

      val notification = status.TaskResult(
        task.job.name,
        MetaTable.getMetaTableDef(task.job.outputTable),
        result.runStatus,
        result.runInfo,
        result.applicationId,
        result.isTransient,
        result.isRawFilesJob,
        result.schemaChanges,
        result.dependencyWarnings,
        Seq.empty,
        notificationTarget.options
      )

      target.connect()
      try {
        target.sendNotification(pipelineInfo, notification)
      } finally {
        target.close()
      }
    } match {
      case Success(_) =>
        None
      case Failure(ex) =>
        log.error(s"$EXCLAMATION Failed to send notifications to '${notificationTarget.name}' for task: ${result.outputTable.name} for '${task.infoDate}'.", ex)
        Option(NotificationFailure(
          task.job.outputTable.name,
          notificationTarget.name,
          task.infoDate,
          ex
        ))
    }
  }

  private[core] def handleSchemaChange(df: DataFrame, table: MetaTable, infoDate: LocalDate): List[SchemaDifference] = {
    if (table.format.isInstanceOf[DataFormat.Raw]) {
      // Raw tables do need schema check
      return List.empty[SchemaDifference]
    }

    val lastSchema = bookkeeper.getLatestSchema(table.name, infoDate)

    lastSchema match {
      case Some((oldSchema, oldInfoDate)) =>
        val diff = compareSchemas(oldSchema, df.schema)
        if (diff.nonEmpty) {
          log.warn(s"$WARNING SCHEMA CHANGE for $table from $oldInfoDate to $infoDate: ${diff.map(_.toString).mkString("; ")}")
          bookkeeper.saveSchema(table.name, infoDate, df.schema)
          SchemaDifference(table.name, oldInfoDate, infoDate, diff) :: Nil
        } else {
          Nil
        }
      case None =>
        bookkeeper.saveSchema(table.name, infoDate, df.schema)
        Nil
    }
  }

  private def getRunInfo(infoDate: LocalDate, started: Instant): Option[RunInfo] = {
    Some(RunInfo(infoDate, started, Instant.now()))
  }

  private def logTaskResult(result: TaskResult, isLazy: Boolean): Unit = synchronized {
    val infoDateMsg = result.runInfo match {
      case Some(date) => s" for ${date.infoDate}"
      case None => ""
    }

    val taskStr = if (isLazy) {
      "Requested lazy task"
    } else {
      "Task"
    }

    val emoji = if (result.runStatus.isFailure) s"$FAILURE" else s"$WARNING"

    val elapsedTimeStr = result.runInfo match {
      case Some(runInfo) =>
        val elapsedTimeMs = runInfo.finished.toEpochMilli - runInfo.started.toEpochMilli
        s" Elapsed time: ${TimeUtils.prettyPrintElapsedTime(elapsedTimeMs)} seconds."
      case None => ""
    }

    result.runStatus match {
      case _: RunStatus.Succeeded =>
        log.info(s"$SUCCESS $taskStr '${result.jobName}'$infoDateMsg has SUCCEEDED.$elapsedTimeStr")
      case RunStatus.ValidationFailed(ex) =>
        log.error(s"$FAILURE $taskStr '${result.jobName}'$infoDateMsg has FAILED VALIDATION.$elapsedTimeStr", ex)
      case RunStatus.Failed(ex) =>
        log.error(s"$FAILURE $taskStr '${result.jobName}'$infoDateMsg has FAILED.$elapsedTimeStr", ex)
      case RunStatus.MissingDependencies(_, tables) =>
        log.error(s"$emoji $taskStr '${result.jobName}'$infoDateMsg has MISSING TABLES: ${tables.mkString(", ")}")
      case RunStatus.FailedDependencies(_, deps) =>
        log.error(s"$emoji $taskStr '${result.jobName}'$infoDateMsg has FAILED DEPENDENCIES: ${deps.map(_.renderText).mkString("; ")}")
      case _: RunStatus.NoData =>
        log.warn(s"$emoji $taskStr '${result.jobName}'$infoDateMsg has NO DATA AT SOURCE.")
      case _: RunStatus.InsufficientData =>
        log.error(s"$FAILURE $taskStr '${result.jobName}'$infoDateMsg has INSUFFICIENT DATA AT SOURCE.")
      case RunStatus.Skipped(msg, _) =>
        log.warn(s"$WARNING $taskStr '${result.jobName}'$infoDateMsg is SKIPPED: $msg.")
      case RunStatus.NotRan =>
        log.info(s"$taskStr '${result.jobName}'$infoDateMsg is SKIPPED.")
    }
  }
}
