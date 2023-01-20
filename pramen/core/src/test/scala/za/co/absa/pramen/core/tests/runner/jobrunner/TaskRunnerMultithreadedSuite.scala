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

package za.co.absa.pramen.core.tests.runner.jobrunner

import com.github.yruslan.channel.Channel
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.RuntimeConfigFactory
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.pipeline.Job
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.job.JobSpy
import za.co.absa.pramen.core.mocks.state.PipelineStateSpy
import za.co.absa.pramen.core.runner.jobrunner.ConcurrentJobRunnerImpl
import za.co.absa.pramen.core.runner.task.RunStatus.{Failed, Succeeded}
import za.co.absa.pramen.core.runner.task.TaskRunnerMultithreaded

import java.time.{Instant, LocalDate, Duration => Dur}

class TaskRunnerMultithreadedSuite extends AnyWordSpec with SparkTestBase {
  import spark.implicits._

  private val runDate = LocalDate.of(2022, 2, 18)

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  "runJob" should {
    "handle a successful single task job" in {
      val (runner, bk, state, job) = getUseCase()

      runner.runJob(job)

      val results = state.completedStatuses

      assert(results.size == 1)
      assert(results.head.runStatus.isInstanceOf[Succeeded])
      assert(results.head.job == job)
      assert(results.head.runInfo.nonEmpty)
      assert(results.head.runInfo.get.infoDate == runDate)
      assert(results.head.schemaChanges.isEmpty)
    }

    "handle a successful multiple task job parallel execution" in {
      val (runner, bk, state, job) = getUseCase(runDate.plusDays(1))

      runner.runJob(job)

      val results = state.completedStatuses

      assert(results.size == 2)
      assert(results.head.runInfo.get.infoDate == runDate)
      assert(results(1).runInfo.get.infoDate == runDate.plusDays(1))
    }

    "handle a failed job parallel execution" in {
      val (runner, bk, state, job) = getUseCase(runFunction = () => throw new IllegalStateException("Test exception"))

      runner.runJob(job)

      val results = state.completedStatuses

      assert(results.size == 1)
      assert(results.head.runStatus.isInstanceOf[Failed])
      assert(results.head.runInfo.get.infoDate == runDate)

      assert(bk.getDataChunks("table_out", runDate, runDate).isEmpty)
    }

    "handle a successful multiple task job sequential execution" in {
      val (runner, bk, state, job) = getUseCase(runDate.plusDays(1), allowParallel = false)

      runner.runJob(job)

      val results = state.completedStatuses

      assert(results.size == 2)
      assert(results.head.runInfo.get.infoDate == runDate)
      assert(results(1).runInfo.get.infoDate == runDate.plusDays(1))
    }

    "handle a failed job sequential execution" in {
      val (runner, bk, state, job) = getUseCase(runFunction = () => throw new IllegalStateException("Test exception"), allowParallel = false)

      runner.runJob(job)

      val results = state.completedStatuses

      assert(results.size == 1)
      assert(results.head.runStatus.isInstanceOf[Failed])
      assert(results.head.runInfo.get.infoDate == runDate)

      assert(bk.getDataChunks("table_out", runDate, runDate).isEmpty)
    }
  }

  "workerLoop" should {
    "run jobs in parallel" in {
      val (runner, bk, state, job) = getUseCase()

      val incomingChan = Channel.make[Job](5)

      runner.startWorkerLoop(incomingChan)

      val completedChan = runner.getCompletedJobsChannel

      incomingChan.send(job)
      incomingChan.send(job)
      incomingChan.close()

      val completed = completedChan.toList

      assert(completed.length == 2)

      val start = Instant.now()
      runner.shutdown()
      val finish = Instant.now()

      assert(Dur.between(start, finish).getSeconds < 5)
    }
  }

  def getUseCase(runDateIn: LocalDate = runDate,
                 isRerun: Boolean = false,
                 runFunction: () => DataFrame = () => exampleDf,
                 allowParallel: Boolean = true
                ): (ConcurrentJobRunnerImpl, Bookkeeper, PipelineStateSpy, Job) = {
    val conf = ConfigFactory.empty()

    val runtimeConfig = RuntimeConfigFactory.getDummyRuntimeConfig(isRerun = isRerun, runDate = runDateIn)

    val bookkeeper = new SyncBookkeeperMock

    val state = new PipelineStateSpy

    bookkeeper.setRecordCount("table_out", runDate.minusDays(1), runDate.minusDays(1), runDate.minusDays(1), 1, 1, 0, 0)

    val stats = MetaTableStats(2, Some(100))

    val job = new JobSpy(runFunction = runFunction, saveStats = stats, allowParallel = allowParallel)

    val taskRunner = new TaskRunnerMultithreaded(conf, bookkeeper, state, runtimeConfig)

    val jobRunner = new ConcurrentJobRunnerImpl(runtimeConfig, bookkeeper, taskRunner)

    (jobRunner, bookkeeper, state, job)
  }

}
