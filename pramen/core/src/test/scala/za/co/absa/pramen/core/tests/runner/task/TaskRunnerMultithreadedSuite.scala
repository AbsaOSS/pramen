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

package za.co.absa.pramen.core.tests.runner.task

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.status.{RunStatus, TaskRunReason}
import za.co.absa.pramen.api.status.RunStatus.Failed
import za.co.absa.pramen.core.RuntimeConfigFactory
import za.co.absa.pramen.core.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.core.mocks.job.JobSpy
import za.co.absa.pramen.core.mocks.journal.JournalMock
import za.co.absa.pramen.core.mocks.lock.TokenLockFactoryMock
import za.co.absa.pramen.core.mocks.state.PipelineStateSpy
import za.co.absa.pramen.core.pipeline.Task
import za.co.absa.pramen.core.runner.task.TaskRunnerMultithreaded

import java.time.LocalDate
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

class TaskRunnerMultithreadedSuite extends AnyWordSpec {
  private val FUTURE_TIMEOUT_SECONDS = 10
  private val infoDate = LocalDate.of(2022, 2, 18)

  "whenEnoughResourcesAreAvailable" should {
    "run the action after acquiring the lock" in {
      val runner = getRunner()
      var done = false

      runner.whenEnoughResourcesAreAvailable(1) {
        this.synchronized{
          done = true
        }
      }

      assert(done)
    }
  }

  "getTruncatedResourceCount" should {
    "return the number of threads requested if it if not bigger than maximum" in {
      val runner = getRunner(parallelTasks = 4)

      val resourcesCount = runner.getTruncatedResourceCount(2)

      assert(resourcesCount == 2)
    }

    "return the maximum if requested too many" in {
      val runner = getRunner(parallelTasks = 4)

      val resourcesCount = runner.getTruncatedResourceCount(8)

      assert(resourcesCount == 4)
    }
  }

  "runParallel" should {
    "return a ready feature for empty sequence of tasks" in {
      val runner = getRunner()

      val tasks = Seq.empty[Task]

      val seqOfFutures = runner.runParallel(tasks)

      assert(seqOfFutures.isEmpty)
    }

    "return a feature of a running task" in {
      val runner = getRunner()

      val tasks = Seq(getTask)

      val futures = runner.runParallel(tasks)

      assert(futures.nonEmpty)
      assert(futures.length == 1)

      val resultTry = Try {
        Await.result(futures.head, Duration(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS))
      }

      assert(!resultTry.isFailure)

      val result: RunStatus = resultTry.get

      assert(result.isInstanceOf[Failed])
    }
  }

  "runSequential" should {
    "return a ready feature for empty sequence of tasks" in {
      val runner = getRunner()

      val tasks = Seq.empty[Task]

      val future = runner.runSequential(tasks)

      assert(future.isCompleted)

      val result = Await.result(future, Duration(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS))

      assert(result.isEmpty)
    }

    "return a feature of a running task" in {
      val runner = getRunner()

      val tasks = Seq(getTask)

      val future = runner.runSequential(tasks)

      val resultTry = Try {
        Await.result(future, Duration(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS))
      }

      assert(!resultTry.isFailure)

      val result: RunStatus = resultTry.get.head

      assert(result.isInstanceOf[Failed])
    }
  }

  "shutdown" should {
    "make threads not runnable afterwards" in {
      val runner = getRunner()

      runner.close()

      assert(runner.executionContext.isShutdown)
    }
  }

  def getRunner(parallelTasks: Int = 1): TaskRunnerMultithreaded = {
    val conf = ConfigFactory.empty()
    val bk = new SyncBookkeeperMock
    val journal = new JournalMock
    val lockFactory = new TokenLockFactoryMock
    val runtimeConfig = RuntimeConfigFactory.getDummyRuntimeConfig(parallelTasks = parallelTasks)
    val pipelineState = new PipelineStateSpy

    new TaskRunnerMultithreaded(conf, bk, journal, lockFactory, pipelineState, runtimeConfig, "abc123")
  }

  def getTask: Task = {
    val job = new JobSpy

    Task(job, infoDate, TaskRunReason.New)
  }
}
