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
import za.co.absa.pramen.core.pipeline.Job
import za.co.absa.pramen.core.runner.splitter.ScheduleStrategyUtils
import za.co.absa.pramen.core.runner.task.{RunStatus, TaskRunner}
import za.co.absa.pramen.core.utils.{Emoji, TimeUtils}

import java.time.{Instant, LocalDate}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}

object TransientJobManager {
  val WARN_UNIONS = 5
  val MAXIMUM_UNIONS = 50

  private val log = LoggerFactory.getLogger(this.getClass)
  private val onDemandJobs = new mutable.HashMap[String, Job]()
  private val runningJobs = new mutable.HashMap[MetastorePartition, Future[DataFrame]]()
  private var taskRunnerOpt: Option[TaskRunner] = None

  private[core] def setTaskRunner(taskRunner_ : TaskRunner): Unit = synchronized {
    taskRunnerOpt = Option(taskRunner_)
  }

  private[core] def hasTaskRunner: Boolean = synchronized {
    taskRunnerOpt.isDefined
  }

  private[core] def addOnDemandJob(job: Job): Unit = synchronized {
    onDemandJobs += job.outputTable.name.toLowerCase -> job
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

  private[core] def selectLatestOnDemandSnapshot(outputTableName: String,
                                                 infoDateUntil: LocalDate): LocalDate = {
    val job = getJob(outputTableName)

    ScheduleStrategyUtils.getLatestActiveInfoDate(outputTableName,
      infoDateUntil,
      job.operation.outputInfoDateExpression,
      job.operation.schedule)
  }

  private[core] def runOnDemandTasks(outputTableName: String,
                                     infoDates: Seq[LocalDate])
                                    (implicit spark: SparkSession): DataFrame = {
    val dfs = infoDates.map(infoDate => runOnDemandTask(outputTableName, infoDate))

    if (dfs.isEmpty) {
      spark.emptyDataFrame
    } else {
      if (infoDates.length > WARN_UNIONS) {
        log.warn(s"${Emoji.WARNING} Performance may be degraded for the task ($outputTableName for ${infoDates.mkString(", ")}) " +
          s"since the number of dataframe unions is too big (${infoDates.length} > $WARN_UNIONS)")
      }
      if (infoDates.length > MAXIMUM_UNIONS) {
        throw new IllegalArgumentException(s"The number of subtasks requested for the on-demand job contains too many " +
          s"dataframe unions (${infoDates.length} > $MAXIMUM_UNIONS)")
      }

      infoDates.tail.foldLeft(runOnDemandTask(outputTableName, infoDates.head))(
        (acc, infoDate) => acc.union(runOnDemandTask(outputTableName, infoDate))
      )
    }
  }

  private[core] def runOnDemandTask(outputTableName: String,
                                    infoDate: LocalDate)
                                   (implicit sparkSession: SparkSession): DataFrame = {
    val start = Instant.now()
    val fut = getOnDemandTaskFuture(outputTableName, infoDate)

    if (!fut.isCompleted) {
      log.info(s"Waiting for the dependent task to finish ($outputTableName for $infoDate)...")
    }
    val df = Await.result(fut, Duration.Inf)
    val finish = Instant.now()
    log.info(s"The task has finished ($outputTableName for $infoDate). Elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")

    df
  }

  private[core] def getOnDemandTaskFuture(outputTableName: String,
                                          infoDate: LocalDate)
                                         (implicit sparkSession: SparkSession): Future[DataFrame] = {
    val metastorePartition = TransientTableManager.getMetastorePartition(outputTableName, infoDate)
    val promise = Promise[DataFrame]()

    val cachedDfFuture = synchronized {
      if (TransientTableManager.hasDataForTheDate(outputTableName, infoDate)) {
        log.info(s"The task ($outputTableName for $infoDate) has the data already.")
        Some(Future.successful(TransientTableManager.getDataForTheDate(outputTableName, infoDate)))
      } else {
        if (runningJobs.contains(metastorePartition)) {
          log.info(s"The task ($outputTableName for $infoDate) is already running. Waiting for results...")
          Some(runningJobs(metastorePartition))
        } else {
          log.info(s"Running the on-demand task ($outputTableName for $infoDate)...")
          runningJobs += metastorePartition -> promise.future
          None
        }
      }
    }

    cachedDfFuture match {
      case Some(fut) =>
        fut
      case None =>
        val job = getJob(outputTableName)
        val fut = promise.future
        val resultTry = try {
          Success(runJob(job, infoDate))
        } catch {
          case ex: Throwable => Failure(ex)
        }
        this.synchronized {
          runningJobs -= metastorePartition
          promise.complete(resultTry)
        }
        fut
    }
  }


  private[core] def getJob(outputTableName: String): Job = {
    val jobOpt = onDemandJobs.get(outputTableName.toLowerCase)

    jobOpt match {
      case Some(job) => job
      case None => throw new IllegalArgumentException(s"On-demand job with output table name '$outputTableName' not found or haven't registered yet.")
    }
  }

  private[core] def reset(): Unit = synchronized {
    onDemandJobs.clear()
    runningJobs.clear()
    taskRunnerOpt = None
  }

  private[core] def runJob(job: Job,
                           infoDate: LocalDate)
                          (implicit sparkSession: SparkSession): DataFrame = {
    taskRunnerOpt match {
      case Some(taskRunner) =>
        taskRunner.runOnDemand(job, infoDate) match {
          case _: RunStatus.Succeeded         => TransientTableManager.getDataForTheDate(job.outputTable.name, infoDate)
          case s: RunStatus.Skipped           => throw new IllegalStateException(s"On-demand job has skipped. ${s.msg}")
          case RunStatus.ValidationFailed(ex) => throw ex
          case RunStatus.Failed(ex)           => throw ex
          case runStatus                      => throw new IllegalStateException(runStatus.getReason().getOrElse("On-demand job failed to run."))
        }
      case None =>
        throw new IllegalStateException("Task runner is not set.")
    }
  }
}
