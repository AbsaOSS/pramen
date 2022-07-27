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
import za.co.absa.pramen.core.app.config.RuntimeConfig
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.pipeline.Job
import za.co.absa.pramen.core.runner.jobrunner.ConcurrentJobRunner.JobRunResults
import za.co.absa.pramen.core.runner.splitter.ScheduleParams
import za.co.absa.pramen.core.runner.task.{RunStatus, TaskResult, TaskRunner}

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext.fromExecutorService
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutorService, Future}
import scala.util.Try
import scala.util.control.NonFatal

class ConcurrentJobRunnerImpl(runtimeConfig: RuntimeConfig,
                              bookkeeper: Bookkeeper,
                              taskRunner: TaskRunner) extends ConcurrentJobRunner {
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
    Await.result(workersFuture, Duration.Inf)
    executionContext.shutdown()
    loopStarted = false
  }

  private def workerLoop(workerNum: Int, incomingJobs: ReadChannel[Job]): Unit = {
    incomingJobs.foreach(job => {
      Try {
        log.info(s"Worker $workerNum starting job '${job.name}' that outputs to '${job.outputTable.name}'...")
        val isSucceeded = runJob(job)

        completedJobsChannel.send((job, Nil, isSucceeded))
      }.recover({
        case NonFatal(ex) =>
          completedJobsChannel.send((job, TaskResult(job, RunStatus.Failed(ex), None, Nil, Nil) :: Nil, false))
      })
    })
    completedJobsChannel.close()
  }

  private[core] def runJob(job: Job): Boolean = {
    val scheduleParams = ScheduleParams.fromRuntimeConfig(runtimeConfig, job.outputTable.trackDays, job.operation.expectedDelayDays)

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

    val statuses = Await.result(fut, Duration.Inf)

    statuses.forall(s => !s.isFailure)
  }

}
