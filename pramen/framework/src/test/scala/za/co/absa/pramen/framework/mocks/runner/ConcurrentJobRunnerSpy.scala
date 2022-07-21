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

package za.co.absa.pramen.framework.mocks.runner

import com.github.yruslan.channel.{Channel, ReadChannel}
import za.co.absa.pramen.framework.pipeline.{Job, TaskRunReason}
import za.co.absa.pramen.framework.runner.jobrunner.ConcurrentJobRunner
import za.co.absa.pramen.framework.runner.jobrunner.ConcurrentJobRunner.JobRunResults
import za.co.absa.pramen.framework.runner.task.{RunInfo, RunStatus, TaskResult}

import java.time.{Instant, LocalDate}
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext.fromExecutorService
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContextExecutorService, Future}

class ConcurrentJobRunnerSpy(includeFails: Boolean = false) extends ConcurrentJobRunner {
  private val completedJobsChannel = Channel.make[JobRunResults](1)

  val infoDate: LocalDate = LocalDate.of(2022, 2, 18)

  val started: Instant = Instant.ofEpochSecond(1645518655)
  val finished: Instant = Instant.ofEpochSecond(1645519655)

  private val executor: ExecutorService = newFixedThreadPool(1)
  implicit private  val executionContext: ExecutionContextExecutorService = fromExecutorService(executor)

  private var workersFuture: Future[Unit] = _

  var workerLoopStartedCount = 0
  var getCompletedChannelCount = 0

  override def startWorkerLoop(incomingJobs: ReadChannel[Job]): Unit = {
    workerLoopStartedCount += 1
    workersFuture = Future {
      workerLoop(incomingJobs)
    }
  }

  override def shutdown(): Unit = {
    Await.ready(workersFuture, Duration(5, SECONDS))
    executionContext.shutdown()
  }

  override def getCompletedJobsChannel: ReadChannel[(Job, Seq[TaskResult], Boolean)] = {
    getCompletedChannelCount += 1
    completedJobsChannel
  }

  private def workerLoop(incomingJobs: ReadChannel[Job]): Unit = {
    var idx = 0
    incomingJobs.foreach(job => {
      val status = if (!includeFails || idx % 3 == 0) {
        RunStatus.Succeeded(Some(1000), 500, Some(10000), TaskRunReason.New)
      } else if (idx % 3 == 1) {
        RunStatus.Failed(new RuntimeException("Dummy exception"))
      } else {
        RunStatus.NoData
      }

      val taskResult = TaskResult(job, status, Some(RunInfo(infoDate, started, finished)), Nil, Nil)

      completedJobsChannel.send((job, taskResult :: Nil, taskResult.runStatus.isInstanceOf[RunStatus.Succeeded] || taskResult.runStatus == RunStatus.NotRan))

      idx += 1
    })
    completedJobsChannel.close()
  }

}
