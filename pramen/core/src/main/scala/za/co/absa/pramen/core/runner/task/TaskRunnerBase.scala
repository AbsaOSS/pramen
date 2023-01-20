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
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{Reason, TaskNotification}
import za.co.absa.pramen.core.app.config.RuntimeConfig
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.exceptions.ReasonException
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.notify.NotificationTargetManager
import za.co.absa.pramen.core.notify.pipeline.SchemaDifference
import za.co.absa.pramen.core.pipeline.JobPreRunStatus._
import za.co.absa.pramen.core.pipeline._
import za.co.absa.pramen.core.state.PipelineState
import za.co.absa.pramen.core.utils.Emoji._
import za.co.absa.pramen.core.utils.SparkUtils._

import java.time.{Instant, LocalDate}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

abstract class TaskRunnerBase(conf: Config,
                              bookkeeper: Bookkeeper,
                              runtimeConfig: RuntimeConfig,
                              pipelineState: PipelineState) extends TaskRunner {
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

  /** Runs multiple tasks in the single thread in the order of info dates. If one task fails, the rest will be skipped. */
  protected def runDependentTasks(tasks: Seq[Task]): Seq[RunStatus] = {
    val sortedTasks = tasks.sortBy(_.infoDate)
    var failedInfoDate: Option[LocalDate] = None

    sortedTasks.map(task =>
      failedInfoDate match {
        case Some(failedDate) =>
          skipTask(task, s"Due to failure for $failedDate")
        case None             =>
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

    val result: TaskResult = validate(task, started) match {
      case Left(failedResult)      => failedResult
      case Right(validationResult) => run(task, started, validationResult)
    }

    onTaskCompletion(task, result)
  }

  /** Skips a task. Performs all task logging and notification sending activities. */
  protected def skipTask(task: Task, reason: String): RunStatus = {
    val now = Instant.now()
    val runStatus = RunStatus.Skipped(reason)
    val runInfo = RunInfo(task.infoDate, now, now)
    val taskResult = TaskResult(task.job, runStatus, Some(runInfo), Nil, Nil, Nil)

    onTaskCompletion(task, taskResult)
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
    val outputTable = task.job.outputTable.name

    Try {
      task.job.preRunCheck(task.infoDate, conf)
    } match {
      case Success(validationResult) =>
        val resultToReturn = validationResult.status match {
          case Ready =>
            log.info(s"Validation of the task: $outputTable for date: ${task.infoDate} is SUCCEEDED.")
            Right(validationResult)
          case NeedsUpdate =>
            log.info(s"The table needs update: $outputTable for date: ${task.infoDate}.")
            Right(validationResult)
          case NoData =>
            log.info(s"NO DATA available for the task: $outputTable for date: ${task.infoDate}.")
            Left(TaskResult(task.job, RunStatus.NoData, getRunInfo(task.infoDate, started), Nil, validationResult.dependencyWarnings, Nil))
          case InsufficientData(actual, expected, oldRecordCount) =>
            log.info(s"INSUFFICIENT DATA available for the task: $outputTable for date: ${task.infoDate}. Expected = $expected, actual = $actual")
            Left(TaskResult(task.job, RunStatus.InsufficientData(actual, expected, oldRecordCount), getRunInfo(task.infoDate, started), Nil, validationResult.dependencyWarnings, Nil))
          case AlreadyRan =>
            if (runtimeConfig.isRerun) {
              log.info(s"RE-RUNNING the task: $outputTable for date: ${task.infoDate}.")
              Right(validationResult)
            } else {
              log.info(s"SKIPPING already ran job: $outputTable for date: ${task.infoDate}.")
              Left(TaskResult(task.job, RunStatus.NotRan, getRunInfo(task.infoDate, started), Nil, validationResult.dependencyWarnings, Nil))
            }
          case FailedDependencies(isFailure, failures) =>
            Left(TaskResult(task.job, RunStatus.FailedDependencies(isFailure, failures), getRunInfo(task.infoDate, started), Nil, Nil, Nil))
        }
        if (validationResult.dependencyWarnings.nonEmpty) {
          log.warn(s"$WARNING Validation of the task: $outputTable for date: ${task.infoDate} has " +
            s"optional dependency failure(s) for table(s): ${validationResult.dependencyWarnings.map(_.table).mkString(", ")} ")
        }
        resultToReturn
      case Failure(ex) =>
        Left(TaskResult(task.job, RunStatus.ValidationFailed(ex), getRunInfo(task.infoDate, started), Nil, Nil, Nil))
    }
  }

  /**
    * Does pre-run checks and vask validations.
    *
    * If validation is successful, in instance of JobPreRunResult is returned.
    * If validation failed an instance of TaskResult is returned.
    *
    * @param task    a task to validate.
    * @param started the instant when the job has started executing.
    * @return an instance of TaskResult on validation failure or optional record count on success.
    */
  private[core] def validate(task: Task, started: Instant): Either[TaskResult, JobPreRunResult] = {
    val outputTable = task.job.outputTable.name

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
                log.info(s"VALIDATION is SUCCESSFUL for the task: $outputTable for date: ${task.infoDate}.")
                Right(status)
              case Reason.NotReady(msg) =>
                log.info(s"NOT READY validation failure for the task: $outputTable for date: ${task.infoDate}. Reason: $msg")
                Left(TaskResult(task.job, RunStatus.ValidationFailed(new ReasonException(Reason.NotReady(msg), msg)), getRunInfo(task.infoDate, started), Nil, status.dependencyWarnings, Nil))
              case Reason.Skip(msg) =>
                log.info(s"SKIP validation failure for the task: $outputTable for date: ${task.infoDate}. Reason: $msg")
                if (bookkeeper.getLatestDataChunk(outputTable, task.infoDate, task.infoDate).isEmpty) {
                  bookkeeper.setRecordCount(outputTable, task.infoDate, task.infoDate, task.infoDate, status.inputRecordsCount.getOrElse(0L), 0, started.getEpochSecond, Instant.now().getEpochSecond)
                }
                Left(TaskResult(task.job, RunStatus.Skipped(msg), getRunInfo(task.infoDate, started), Nil, status.dependencyWarnings, Nil))
            }
          case Failure(ex) =>
            Left(TaskResult(task.job, RunStatus.ValidationFailed(ex), getRunInfo(task.infoDate, started), Nil, status.dependencyWarnings, Nil))
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
    Try {
      val recordCountOldOpt = bookkeeper.getLatestDataChunk(task.job.outputTable.name, task.infoDate, task.infoDate).map(_.outputRecordCount)

      val dfOut = task.job.run(task.infoDate, conf)

      val schemaChangesBeforeTransform = handleSchemaChange(dfOut, task.job.outputTable.name, task.infoDate)

      val dfWithTimestamp = task.job.operation.processingTimestampColumn match {
        case Some(timestampCol) => addProcessingTimestamp(dfOut, timestampCol)
        case None => dfOut
      }

      val postProcessed = task.job.postProcessing(dfWithTimestamp, task.infoDate, conf)

      val dfTransformed = applyFilters(
        applyTransformations(postProcessed, task.job.operation.schemaTransformations),
        task.job.operation.filters,
        task.infoDate,
        task.infoDate,
        task.infoDate
      )

      val schemaChangesAfterTransform = if (task.job.operation.schemaTransformations.nonEmpty) {
        handleSchemaChange(dfTransformed, s"${task.job.outputTable.name}_transformed", task.infoDate)
      } else {
        Nil
      }

      val stats = if (runtimeConfig.isDryRun) {
        log.warn(s"$WARNING DRY RUN mode, no actual writes to ${task.job.outputTable.name} for ${task.infoDate} will be performed.")
        MetaTableStats(dfTransformed.count(), None)
      } else {
        task.job.save(dfTransformed, task.infoDate, conf, started, validationResult.inputRecordsCount)
      }

      val finished = Instant.now()

      val completionReason = if (validationResult.status == NeedsUpdate || (validationResult.status == AlreadyRan && task.reason != TaskRunReason.Rerun))
        TaskRunReason.Update else task.reason

      TaskResult(task.job,
        RunStatus.Succeeded(recordCountOldOpt, stats.recordCount, stats.dataSizeBytes, completionReason),
        Some(RunInfo(task.infoDate, started, finished)),
        schemaChangesBeforeTransform ::: schemaChangesAfterTransform,
        validationResult.dependencyWarnings,
        Seq.empty)
    } match {
      case Success(result) =>
        result
      case Failure(ex) =>
        TaskResult(task.job, RunStatus.Failed(ex), getRunInfo(task.infoDate, started), Nil,
          validationResult.dependencyWarnings, Nil)
    }
  }

  /** Logs task completion and sends corresponding notifications. */
  private def onTaskCompletion(task: Task, taskResult: TaskResult): RunStatus = {
    val notificationTargetErrors = sendNotifications(task, taskResult)
    val updatedResult = taskResult.copy(notificationTargetErrors = notificationTargetErrors)

    logTaskResult(updatedResult)
    pipelineState.addTaskCompletion(Seq(updatedResult))

    updatedResult.runStatus
  }

  private def sendNotifications(task: Task, result: TaskResult): Seq[NotificationFailure] = {
    task.job.notificationTargets.flatMap(notificationTarget => sendNotifications(task, result, notificationTarget))
  }

  private def sendNotifications(task: Task, result: TaskResult, notificationTarget: JobNotificationTarget): Option[NotificationFailure] = {
    Try {
      val target = notificationTarget.target

      NotificationTargetManager.runStatusToTaskStatus(result.runStatus).foreach { taskStatus =>
        val notification = TaskNotification(
          task.job.outputTable.name,
          task.infoDate,
          result.runInfo.get.started,
          result.runInfo.get.finished,
          taskStatus,
          notificationTarget.options
        )

        target.connect()
        try {
          target.sendNotification(notification)
        } finally {
          target.close()
        }
      }
    } match {
      case Success(_) =>
        None
      case Failure(ex) =>
        log.error(s"$EXCLAMATION Failed to send notifications to '${notificationTarget.name}' for task: ${result.job.outputTable.name} for '${task.infoDate}'.", ex)
        Option(NotificationFailure(
          task.job.outputTable.name,
          notificationTarget.name,
          task.infoDate,
          ex
        ))
    }
  }

  private[core] def handleSchemaChange(df: DataFrame, table: String, infoDate: LocalDate): List[SchemaDifference] = {
    val lastSchema = bookkeeper.getLatestSchema(table, infoDate.minusDays(1))

    lastSchema match {
      case Some((oldSchema, oldInfoDate)) =>
        val diff = compareSchemas(oldSchema, df.schema)
        if (diff.nonEmpty) {
          log.warn(s"$WARNING SCHEMA CHANGE for $table from $oldInfoDate to $infoDate: ${diff.map(_.toString).mkString("; ")}")
          bookkeeper.saveSchema(table, infoDate, df.schema)
          SchemaDifference(table, oldInfoDate, infoDate, diff) :: Nil
        } else {
          Nil
        }
      case None =>
        bookkeeper.saveSchema(table, infoDate, df.schema)
        Nil
    }
  }

  private def getRunInfo(infoDate: LocalDate, started: Instant): Option[RunInfo] = {
    Some(RunInfo(infoDate, started, Instant.now()))
  }

  private def logTaskResult(result: TaskResult): Unit = synchronized {
    val infoDateMsg = result.runInfo match {
      case Some(date) => s" for $date"
      case None => ""
    }
    result.runStatus match {
      case _: RunStatus.Succeeded =>
        log.info(s"$SUCCESS Task '${result.job.name}'$infoDateMsg has SUCCEEDED.")
      case RunStatus.ValidationFailed(ex) =>
        log.warn(s"$FAILURE Task '${result.job.name}'$infoDateMsg has FAILED VALIDATION", ex)
      case RunStatus.Failed(ex) =>
        log.error(s"$FAILURE Task '${result.job.name}'$infoDateMsg has FAILED", ex)
      case RunStatus.MissingDependencies(isFailure, tables) =>
        val emoji = if (isFailure) "$FAILURE" else "$WARNING"
        log.warn(s"$emoji Task '${result.job.name}'$infoDateMsg has MISSING TABLES: ${tables.mkString(", ")}")
      case RunStatus.FailedDependencies(isFailure, deps) =>
        val emoji = if (isFailure) s"$FAILURE" else s"$WARNING"
        log.warn(s"$emoji Task '${result.job.name}'$infoDateMsg has FAILED DEPENDENCIES: ${deps.map(_.renderText).mkString("; ")}")
      case RunStatus.NoData =>
        log.info(s"$FAILURE Task '${result.job.name}'$infoDateMsg has NO DATA AT SOURCE.")
      case _: RunStatus.InsufficientData =>
        log.info(s"$FAILURE Task '${result.job.name}'$infoDateMsg has INSUFFICIENT DATA AT SOURCE.")
      case RunStatus.Skipped(msg) =>
        log.info(s"$WARNING Task '${result.job.name}'$infoDateMsg is SKIPPED: $msg.")
      case RunStatus.NotRan =>
        log.info(s"Task '${result.job.name}'$infoDateMsg is SKIPPED.")
    }
  }
}
