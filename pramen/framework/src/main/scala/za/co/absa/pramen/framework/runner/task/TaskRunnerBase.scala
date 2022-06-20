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

package za.co.absa.pramen.framework.runner.task

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.api.metastore.MetaTableStats
import za.co.absa.pramen.framework.app.config.RuntimeConfig
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.exceptions.ReasonException
import za.co.absa.pramen.framework.job.v2.job.JobPreRunStatus._
import za.co.absa.pramen.framework.job.v2.job._
import za.co.absa.pramen.framework.notify.SchemaDifference
import za.co.absa.pramen.framework.utils.SparkUtils._

import java.time.{Instant, LocalDate}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

abstract class TaskRunnerBase(conf: Config,
                              bookkeeper: SyncBookKeeper,
                              runtimeConfig: RuntimeConfig) extends TaskRunner {
  implicit private val ecDefault: ExecutionContext = ExecutionContext.global

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Runs tasks and returns their futures. Subclasses should override this method.
    *
    * @param tasks Tasks to run.
    * @return A sequence of futures - one for each task.
    */
  def runAllTasks(tasks: Seq[Task]): Seq[Future[RunStatus]]

  override def runJobTasks(job: Job, infoDates: Seq[TaskPreDef]): Future[Seq[RunStatus]] = {
    val tasks = infoDates.map(p => Task(job, p.infoDate, p.reason))

    val futures = runAllTasks(tasks)

    Future.sequence(futures)
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
  def preRunCheck(task: Task, started: Instant): Either[TaskResult, JobPreRunResult] = {
    val outputTable = task.job.outputTable.name

    Try {
      task.job.preRunCheck(task.infoDate, conf)
    } match {
      case Success(validationResult) =>
        validationResult.status match {
          case Ready                        =>
            log.info(s"Validation of the task: $outputTable for date: ${task.infoDate} is SUCCEEDED.")
            Right(validationResult)
          case NeedsUpdate                  =>
            log.info(s"The task needs update: $outputTable for date: ${task.infoDate}.")
            Right(validationResult)
          case NoData                       =>
            log.info(s"NO DATA available for the task: $outputTable for date: ${task.infoDate}.")
            Left(TaskResult(task.job, RunStatus.NoData, getRunInfo(task.infoDate, started), Nil, validationResult.dependencyWarnings))
          case AlreadyRan                   =>
            if (runtimeConfig.isRerun) {
              log.info(s"RE-RUNNING the task: $outputTable for date: ${task.infoDate}.")
              Right(validationResult)
            } else {
              log.info(s"SKIPPING already ran job: $outputTable for date: ${task.infoDate}.")
              Left(TaskResult(task.job, RunStatus.NotRan, getRunInfo(task.infoDate, started), Nil, validationResult.dependencyWarnings))
            }
          case FailedDependencies(failures) =>
            Left(TaskResult(task.job, RunStatus.FailedDependencies(failures), getRunInfo(task.infoDate, started), Nil, Nil))
        }
      case Failure(ex)               =>
        Left(TaskResult(task.job, RunStatus.ValidationFailed(ex), getRunInfo(task.infoDate, started), Nil, Nil))
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
  def validate(task: Task, started: Instant): Either[TaskResult, JobPreRunResult] = {
    val outputTable = task.job.outputTable.name

    preRunCheck(task, started) match {
      case Left(result)  =>
        Left(result)
      case Right(status) =>
        Try {
          task.job.validate(task.infoDate, conf)
        } match {
          case Success(validationResult) =>
            validationResult match {
              case Reason.Ready         =>
                log.info(s"VALIDATION is SUCCESSFUL for the task: $outputTable for date: ${task.infoDate}.")
                Right(status)
              case Reason.NotReady(msg) =>
                log.info(s"NOT READY validation failure for the task: $outputTable for date: ${task.infoDate}. Reason: $msg")
                Left(TaskResult(task.job, RunStatus.ValidationFailed(new ReasonException(Reason.NotReady(msg), msg)), getRunInfo(task.infoDate, started), Nil, status.dependencyWarnings))
              case Reason.Skip(msg)     =>
                log.info(s"SKIP validation failure for the task: $outputTable for date: ${task.infoDate}. Reason: $msg")
                if (bookkeeper.getLatestDataChunk(outputTable, task.infoDate, task.infoDate).isEmpty) {
                  bookkeeper.setRecordCount(outputTable, task.infoDate, task.infoDate, task.infoDate, status.inputRecordsCount.getOrElse(0L), 0, started.getEpochSecond, Instant.now().getEpochSecond)
                }
                Left(TaskResult(task.job, RunStatus.Skipped(msg), getRunInfo(task.infoDate, started), Nil, status.dependencyWarnings))
            }
          case Failure(ex)               =>
            Left(TaskResult(task.job, RunStatus.ValidationFailed(ex), getRunInfo(task.infoDate, started), Nil, status.dependencyWarnings))
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
  def run(task: Task, started: Instant, validationResult: JobPreRunResult): TaskResult = {
    Try {
      val recordCountOldOpt = bookkeeper.getLatestDataChunk(task.job.outputTable.name, task.infoDate, task.infoDate).map(_.outputRecordCount)

      val dfOut = task.job.run(task.infoDate, conf)

      val schemaChangesBeforeTransform = handleSchemaChange(dfOut, task.job.outputTable.name, task.infoDate)

      val dfWithTimestamp = task.job.operation.processingTimestampColumn match {
        case Some(timestampCol) => addProcessingTimestamp(dfOut, timestampCol)
        case None               => dfOut
      }

      val postProcessed = task.job.postProcessing(dfWithTimestamp, task.infoDate, conf)

      val dfTransformed = applyFilters(
        applyTransformations(postProcessed, task.job.operation.schemaTransformations),
        task.job.operation.filters,
        task.infoDate
      )

      val schemaChangesAfterTransform = if (task.job.operation.schemaTransformations.nonEmpty) {
        handleSchemaChange(dfTransformed, s"${task.job.outputTable.name}_transformed", task.infoDate)
      } else {
        Nil
      }

      val stats = if (runtimeConfig.isDryRun) {
        log.info(s"DRY RUN mode, no actual writes to ${task.job.outputTable.name} for ${task.infoDate} will be performed.")
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
        validationResult.dependencyWarnings)
    } match {
      case Success(result) =>
        result
      case Failure(ex)     =>
        TaskResult(task.job, RunStatus.Failed(ex), getRunInfo(task.infoDate, started), Nil,
          validationResult.dependencyWarnings)
    }
  }

  private[framework] def handleSchemaChange(df: DataFrame, table: String, infoDate: LocalDate): List[SchemaDifference] = {
    val lastSchema = bookkeeper.getLatestSchema(table, infoDate.minusDays(1))

    lastSchema match {
      case Some((oldSchema, oldInfoDate)) =>
        val diff = compareSchemas(oldSchema, df.schema)
        if (diff.nonEmpty) {
          log.warn(s"SCHEMA CHANGE for $table from $oldInfoDate to $infoDate: ${diff.map(_.toString).mkString("; ")}")
          bookkeeper.saveSchema(table, infoDate, df.schema)
          SchemaDifference(table, oldInfoDate, infoDate, diff) :: Nil
        } else {
          Nil
        }
      case None                           =>
        bookkeeper.saveSchema(table, infoDate, df.schema)
        Nil
    }
  }

  private def getRunInfo(infoDate: LocalDate, started: Instant): Option[RunInfo] = {
    Some(RunInfo(infoDate, started, Instant.now()))
  }

  protected def logTaskResult(result: TaskResult): Unit = synchronized {
    val infoDateMsg = result.runInfo match {
      case Some(date) => s" for $date"
      case None       => ""
    }
    result.runStatus match {
      case _: RunStatus.Succeeded                =>
        log.info(s"Task '${result.job.name}'$infoDateMsg has SUCCEEDED.")
      case RunStatus.ValidationFailed(ex)        =>
        log.warn(s"Task '${result.job.name}'$infoDateMsg has FAILED VALIDATION", ex)
      case RunStatus.Failed(ex)                  =>
        log.error(s"Task '${result.job.name}'$infoDateMsg has FAILED", ex)
      case RunStatus.MissingDependencies(tables) =>
        log.warn(s"Task '${result.job.name}'$infoDateMsg has MISSING TABLES: ${tables.mkString(", ")}")
      case RunStatus.FailedDependencies(deps)    =>
        log.warn(s"Task '${result.job.name}'$infoDateMsg has MISSING DEPENDENCIES: ${deps.flatMap(d => d.failedTables).distinct.mkString(", ")}")
      case RunStatus.NoData                      =>
        log.info(s"Task '${result.job.name}'$infoDateMsg has NO DATA AT SOURCE.")
      case RunStatus.Skipped(msg)                =>
        log.info(s"Task '${result.job.name}'$infoDateMsg is SKIPPED: $msg.")
      case RunStatus.NotRan                      =>
        log.info(s"Task '${result.job.name}'$infoDateMsg is SKIPPED.")
    }
  }
}
