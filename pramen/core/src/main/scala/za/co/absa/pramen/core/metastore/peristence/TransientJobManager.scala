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

package za.co.absa.pramen.core.metastore.peristence

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.status.RunStatus
import za.co.absa.pramen.core.pipeline.Job
import za.co.absa.pramen.core.runner.splitter.ScheduleStrategyUtils
import za.co.absa.pramen.core.runner.task.TaskRunner
import za.co.absa.pramen.core.utils.{Emoji, TimeUtils}

import java.time.{Instant, LocalDate}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Try}

object TransientJobManager {
  val WARN_UNIONS = 5
  val MAXIMUM_UNIONS = 50

  private val log = LoggerFactory.getLogger(this.getClass)
  private val lazyJobs = new mutable.HashMap[String, Job]()
  private val runningJobs = new mutable.HashMap[MetastorePartition, Future[DataFrame]]()
  private var taskRunnerOpt: Option[TaskRunner] = None

  private[core] def setTaskRunner(taskRunner_ : TaskRunner): Unit = synchronized {
    taskRunnerOpt = Option(taskRunner_)
  }

  private[core] def hasTaskRunner: Boolean = synchronized {
    taskRunnerOpt.isDefined
  }

  private[core] def addLazyJob(job: Job): Unit = synchronized {
    lazyJobs += job.outputTable.name.toLowerCase -> job
  }

  private[core] def selectInfoDatesToExecute(outputTableName: String,
                                             infoDateFrom: LocalDate,
                                             infoDateTo: LocalDate): Seq[LocalDate] = {
    val job = getJob(outputTableName)

    ScheduleStrategyUtils.getActiveInfoDates(outputTableName,
      infoDateFrom,
      infoDateTo,
      job.operation.outputInfoDateExpression,
      job.operation.schedule)
  }

  private[core] def selectLatestLazySnapshot(outputTableName: String,
                                             infoDateUntil: LocalDate): LocalDate = {
    val job = getJob(outputTableName)

    ScheduleStrategyUtils.getLatestActiveInfoDate(outputTableName,
      infoDateUntil,
      job.operation.outputInfoDateExpression,
      job.operation.schedule)
  }

  private[core] def runLazyTasks(outputTableName: String,
                                 infoDates: Seq[LocalDate])
                                (implicit spark: SparkSession): DataFrame = {
    val dfs = infoDates.map(infoDate => runLazyTask(outputTableName, infoDate))

    if (dfs.isEmpty) {
      TransientTableManager.getEmptyDfForTable(outputTableName).getOrElse(spark.emptyDataFrame)
    } else {
      if (infoDates.length > WARN_UNIONS && infoDates.length <= MAXIMUM_UNIONS) {
        log.warn(s"${Emoji.WARNING} Performance may be degraded for the task ($outputTableName for ${infoDates.mkString(", ")}) " +
          s"since the number of dataframe unions is too big (${infoDates.length} > $WARN_UNIONS)")
      }
      if (infoDates.length > MAXIMUM_UNIONS) {
        throw new IllegalArgumentException(s"The number of subtasks requested for the lazy job contains too many " +
          s"dataframe unions (${infoDates.length} > $MAXIMUM_UNIONS)")
      }

      infoDates.tail.foldLeft(runLazyTask(outputTableName, infoDates.head))(
        (acc, infoDate) => {
          val df = runLazyTask(outputTableName, infoDate)
          safeUnion(acc, df)
        }
      )
    }
  }

  def safeUnion(df1: DataFrame, df2: DataFrame): DataFrame = {
    val df1Empty = df1.schema.fields.isEmpty
    val df2Empty = df2.schema.fields.isEmpty

    if (df1Empty && df2Empty) {
      df1
    } else if (!df1Empty && df2Empty) {
      df1
    } else if (df1Empty && !df2Empty) {
      df2
    } else {
      df1.union(df2)
    }
  }

  private[core] def runLazyTask(outputTableName: String,
                                infoDate: LocalDate)
                               (implicit spark: SparkSession): DataFrame = {
    val start = Instant.now()
    val fut = getLazyTaskFuture(outputTableName, infoDate)

    if (!fut.isCompleted) {
      log.info(s"Waiting for the dependent task to finish ($outputTableName for $infoDate)...")
    }

    val df = Await.result(fut, Duration.Inf)
    val finish = Instant.now()
    log.info(s"The task has finished ($outputTableName for $infoDate). Elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")

    df
  }

  private[core] def getLazyTaskFuture(outputTableName: String,
                                      infoDate: LocalDate)
                                     (implicit spark: SparkSession): Future[DataFrame] = {

    val promise = Promise[DataFrame]()

    val cachedDfFuture = synchronized {
      if (TransientTableManager.hasDataForTheDate(outputTableName, infoDate)) {
        log.info(s"The task ($outputTableName for $infoDate) has the data already.")
        Some(Future.successful(TransientTableManager.getDataForTheDate(outputTableName, infoDate)))
      } else {
        testAndSetRunningJobFuture(outputTableName, infoDate, promise.future)
      }
    }

    cachedDfFuture match {
      case Some(fut) =>
        fut
      case None =>
        val job = getJob(outputTableName)
        val fut = promise.future
        val resultTry = try {
          Try(runJob(job, infoDate))
        } catch {
          case ex: Throwable => Failure(ex)
        }
        this.synchronized {
          removeRunningJobFuture(outputTableName, infoDate)
          promise.complete(resultTry)
        }
        fut
    }
  }

  /**
    * Checks if a transient job that outputs to the specified output table for the specified information date
    * is already running. If yes, returns the future to keep track of the resulting dataframe.
    * If the job is not running, add the future provided for the new job execution, and add it to the
    * map of running jobs.
    * The logic is designed to work in a multi-threaded environments, that's why the check and the shared state
    * modifications are performed inside a single synchronized block.
    *
    * @param outputTableName The output table of the job.
    * @param infoDate        The information date of the job execution.
    * @param newJonFuture    The future to register as a running job if no other jobs are running.
    * @return The future of the job that is already running in case there is one.
    */
  private[core] def testAndSetRunningJobFuture(outputTableName: String,
                                               infoDate: LocalDate,
                                               newJonFuture: Future[DataFrame]): Option[Future[DataFrame]] = synchronized {
    val metastorePartition = TransientTableManager.getMetastorePartition(outputTableName, infoDate)

    if (runningJobs.contains(metastorePartition)) {
      log.info(s"The task ($outputTableName for $infoDate) is already running. Waiting for results...")
      Some(runningJobs(metastorePartition))
    } else {
      log.info(s"Running (materializing) the lazy task ($outputTableName for $infoDate)...")
      runningJobs += metastorePartition -> newJonFuture
      None
    }
  }

  private[core] def removeRunningJobFuture(outputTableName: String,
                                           infoDate: LocalDate
                                          ): Unit = synchronized {
    val metastorePartition = TransientTableManager.getMetastorePartition(outputTableName, infoDate)

    runningJobs -= metastorePartition
  }

  private[core] def getJob(outputTableName: String): Job = {
    val jobOpt = lazyJobs.get(outputTableName.toLowerCase)

    jobOpt match {
      case Some(job) => job
      case None => throw new IllegalArgumentException(s"Lazy job with output table name '$outputTableName' not found or haven't registered yet.")
    }
  }

  private[core] def reset(): Unit = synchronized {
    lazyJobs.clear()
    runningJobs.clear()
    taskRunnerOpt = None
  }

  private[core] def runJob(job: Job,
                           infoDate: LocalDate)
                          (implicit spark: SparkSession): DataFrame = {
    val jobStr = s"Lazy job outputting to '${job.outputTable.name}' for '$infoDate'"
    taskRunnerOpt match {
      case Some(taskRunner) =>
        taskRunner.runLazyTask(job, infoDate) match {
          case _: RunStatus.Succeeded         => TransientTableManager.getDataForTheDate(job.outputTable.name, infoDate)
          case _: RunStatus.Skipped           => spark.emptyDataFrame
          case RunStatus.ValidationFailed(ex) => throw new IllegalStateException(s"$jobStr validation failed.", ex)
          case RunStatus.Failed(ex)           => throw new IllegalStateException(s"$jobStr failed.", ex)
          case runStatus                      => throw new IllegalStateException(s"$jobStr failed to run ${runStatus.getReason.map(a => s"($a)").getOrElse("")}.")
        }
      case None =>
        throw new IllegalStateException("Task runner is not set.")
    }
  }
}
