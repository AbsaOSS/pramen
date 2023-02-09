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
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.app.config.RuntimeConfig
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.journal.Journal
import za.co.absa.pramen.core.pipeline.Task
import za.co.absa.pramen.core.state.PipelineState

import java.util.concurrent.{ExecutorService, Semaphore}
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext.fromExecutorService
import scala.concurrent.{ExecutionContextExecutorService, Future}
import scala.util.Try

/**
  * The responsibility of this class is to handle the execution method.
  * This class runs tasks in multiple threads.
  */
class TaskRunnerMultithreaded(conf: Config,
                              bookkeeper: Bookkeeper,
                              journal: Journal,
                              pipelineState: PipelineState,
                              runtimeConfig: RuntimeConfig) extends TaskRunnerBase(conf, bookkeeper, journal, runtimeConfig, pipelineState) {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val executor: ExecutorService = newFixedThreadPool(runtimeConfig.parallelTasks)
  implicit private val executionContext: ExecutionContextExecutorService = fromExecutorService(executor)

  private val maxResources = runtimeConfig.parallelTasks
  private val availableResources: Semaphore = new Semaphore(maxResources, true)

  /**
    * We want to prevent a case when a lot of resource-intensive actions are running at the same time.
    * We have a finite number of resources. Before executing, the *action* asks for the required resources and only when
    * there is enough of them available, the action will be executed.
    */
  private[task] def whenEnoughResourcesAreAvailable[T](requestedCount: Int)(action: => T): T = {
    val resourceCount = getTruncatedResourceCount(requestedCount)

    availableResources.acquire(resourceCount)
    val result = Try { action }
    availableResources.release(resourceCount)

    result.get
  }

  private[task] def getTruncatedResourceCount(requestedCount: Int): Int = {
    if (requestedCount > maxResources) {
      log.warn(s"Asked for $requestedCount resources but maximum allowed is $maxResources. Truncating to $maxResources")
      maxResources
    } else {
      requestedCount
    }
  }

  override def runParallel(tasks: Seq[Task]): Seq[Future[RunStatus]] = {
    tasks.map(task => Future {
      whenEnoughResourcesAreAvailable(task.job.operation.consumeThreads) {
        runTask(task)
      }
    })
  }

  override def runSequential(tasks: Seq[Task]): Future[Seq[RunStatus]] = {
    Future {
      val requiredResources = tasks.map(_.job.operation.consumeThreads).max
      whenEnoughResourcesAreAvailable(requiredResources) {
        runDependentTasks(tasks)
      }
    }
  }

  override def shutdown(): Unit = {
    executionContext.shutdown()
  }

}
