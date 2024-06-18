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

package za.co.absa.pramen.core.runner.jobrunner

import com.github.yruslan.channel.{Channel, ReadChannel}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.DataFormat
import za.co.absa.pramen.api.status.{RunStatus, TaskResult}
import za.co.absa.pramen.core.app.config.RuntimeConfig
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.exceptions.FatalErrorWrapper
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.metastore.peristence.TransientJobManager
import za.co.absa.pramen.core.pipeline.Job
import za.co.absa.pramen.core.runner.jobrunner.ConcurrentJobRunner.JobRunResults
import za.co.absa.pramen.core.runner.splitter.ScheduleParams
import za.co.absa.pramen.core.runner.task.TaskRunner
import za.co.absa.pramen.core.utils.Emoji

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext.fromExecutorService
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutorService, Future}
import scala.util.control.NonFatal

class ConcurrentJobRunnerImpl(runtimeConfig: RuntimeConfig,
                              bookkeeper: Bookkeeper,
                              taskRunner: TaskRunner,
                              applicationId: String) extends ConcurrentJobRunner {
  private val log = LoggerFactory.getLogger(this.getClass)

  private var loopStarted = false
  private val completedJobsChannel = Channel.make[JobRunResults](1)
  private var workersFuture: Future[Unit] = _

  private val executor: ExecutorService = newFixedThreadPool(runtimeConfig.parallelTasks)
  implicit private  val executionContext: ExecutionContextExecutorService = fromExecutorService(executor)

  override def getCompletedJobsChannel: ReadChannel[JobRunResults] = {
    completedJobsChannel
  }

  override def startWorkerLoop(incomingJobs: ReadChannel[Job]): Unit = {
    if (loopStarted) {
      throw new IllegalStateException("Worker loop already started")
    }
    loopStarted = true

    val workers = Range(0, runtimeConfig.parallelTasks).map(workerNum => {
      Future {
        workerLoop(workerNum, incomingJobs)
      }
    })

    workersFuture = Future.sequence(workers).map(_ => ())
  }

  def shutdown(): Unit = {
    if (!loopStarted) {
      throw new IllegalStateException("Worker loop hasn't started yet")
    }
    log.info("Waiting for worker threads to finish...")
    Await.result(workersFuture, Duration.Inf)
    log.info("Workers have finished. Shutting down the execution context...")
    executionContext.shutdown()
    log.info("The execution context is now finished.")
    loopStarted = false
  }

  private def workerLoop(workerNum: Int, incomingJobs: ReadChannel[Job]): Unit = {
    incomingJobs.foreach { job =>
      val isTransient = job.outputTable.format.isTransient
      try {
        log.info(s"Worker $workerNum starting job '${job.name}' that outputs to '${job.outputTable.name}'...")
        val isSucceeded = runJob(job)

        completedJobsChannel.send((job, Nil, isSucceeded))
      } catch {
        case ex: FatalErrorWrapper if ex.cause != null => onFatalException(ex.cause, job, isTransient)
        case NonFatal(ex)                              => sendFailure(ex, job, isTransient)
        case ex: Throwable                             => onFatalException(ex, job, isTransient)
      }
    }
    completedJobsChannel.close()
  }

  private[core] def onFatalException(ex: Throwable, job: Job, isTransient: Boolean): Unit = {
    log.error(s"${Emoji.FAILURE} A FATAL error has been encountered.", ex)
    val fatalEx = new FatalErrorWrapper(s"FATAL exception encountered, stopping the pipeline.", ex)
    sendFailure(fatalEx, job, isTransient)
  }

  private[core] def sendFailure(ex: Throwable, job: Job, isTransient: Boolean): Unit = {
    completedJobsChannel.send((job,
      TaskResult(job.name,
        MetaTable.getMetaTableDef(job.outputTable),
        RunStatus.Failed(ex),
        None,
        applicationId,
        isTransient,
        job.outputTable.format.isInstanceOf[DataFormat.Raw],
        Nil,
        Nil,
        Nil,
        job.operation.extraOptions
      ) :: Nil, false))
  }

  private[core] def runJob(job: Job): Boolean = {
    if (job.outputTable.format.isLazy) {
      runLazyJob(job)
    } else {
      runEagerJob(job)
    }
  }

  private[core] def runEagerJob(job: Job): Boolean = {
    val trackDays = job.trackDays
    log.info(s"Effective track days for ${job.name} outputting to ${job.outputTable.name} = $trackDays")

    val scheduleParams = ScheduleParams.fromRuntimeConfig(runtimeConfig, trackDays, job.operation.expectedDelayDays)

    val taskDefs = job.scheduleStrategy.getDaysToRun(
      job.outputTable.name,
      job.operation.dependencies,
      bookkeeper,
      job.operation.outputInfoDateExpression,
      job.operation.schedule,
      scheduleParams,
      job.operation.initialSourcingDateExpression,
      job.outputTable.infoDateStart
    )

    log.info(s"Dates selected to run for '${job.outputTable.name}': ${taskDefs.map(_.infoDate).mkString(", ")}")

    val fut = taskRunner.runJobTasks(job, taskDefs)

    log.info("Waiting for all job tasks to finish...")
    val statuses = Await.result(fut, Duration.Inf)
    log.info("All job tasks have finished.")

    // Rethrow fatal errors so the pipeline can be stopped asap.
    statuses.foreach {
      case RunStatus.Failed(ex) if ex.isInstanceOf[FatalErrorWrapper] => throw ex
      case _ => // skip
    }

    statuses.forall(s => !s.isFailure)
  }

  private[core] def runLazyJob(job: Job): Boolean = {
    TransientJobManager.addLazyJob(job)
    true
  }

}
