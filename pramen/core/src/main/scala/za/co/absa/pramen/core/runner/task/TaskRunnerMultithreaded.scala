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
import za.co.absa.pramen.api.status.RunStatus
import za.co.absa.pramen.core.app.config.RuntimeConfig
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.exceptions.FatalErrorWrapper
import za.co.absa.pramen.core.journal.Journal
import za.co.absa.pramen.core.lock.TokenLockFactory
import za.co.absa.pramen.core.pipeline.Task
import za.co.absa.pramen.core.state.PipelineState
import za.co.absa.pramen.core.utils.Emoji

import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.{ExecutorService, Semaphore}
import scala.concurrent.ExecutionContext.fromExecutorService
import scala.concurrent.{ExecutionContextExecutorService, Future}
import scala.util.control.NonFatal

/**
  * The responsibility of this class is to handle the execution method.
  * This class runs tasks in multiple threads.
  */
class TaskRunnerMultithreaded(conf: Config,
                              bookkeeper: Bookkeeper,
                              journal: Journal,
                              lockFactory: TokenLockFactory,
                              pipelineState: PipelineState,
                              runtimeConfig: RuntimeConfig,
                              applicationId: String) extends TaskRunnerBase(conf, bookkeeper, journal, lockFactory, runtimeConfig, pipelineState, applicationId) {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val executor: ExecutorService = newFixedThreadPool(runtimeConfig.parallelTasks)
  implicit private[core] val executionContext: ExecutionContextExecutorService = fromExecutorService(executor)

  private val maxResources = runtimeConfig.parallelTasks
  private val availableResources: Semaphore = new Semaphore(maxResources, true)

  /**
    * We want to prevent a case when a lot of resource-intensive actions are running at the same time.
    * We have a finite number of resources. Before executing, the *action* asks for the required resources and only when
    * there is enough of them available, the action will be executed.
    */
  private[core] def whenEnoughResourcesAreAvailable[T](requestedCount: Int)(action: => T): T = {
    val resourceCount = getTruncatedResourceCount(requestedCount)

    availableResources.acquire(resourceCount)
    val result = try {
      action
    } finally  {
      availableResources.release(resourceCount)
    }

    result
  }

  private[core] def getTruncatedResourceCount(requestedCount: Int): Int = {
    if (requestedCount > maxResources) {
      log.warn(s"${Emoji.WARNING} Asked for $requestedCount resources but maximum allowed is $maxResources. Truncating to $maxResources")
      maxResources
    } else {
      requestedCount
    }
  }

  override def runParallel(tasks: Seq[Task]): Seq[Future[RunStatus]] = {
    tasks.map(task => Future {
      log.warn(s"${Emoji.PARALLEL}The task has requested ${task.job.operation.consumeThreads} threads...")

      // We cannot use 'Try' here, need to use 'try' to catch fatal errors.
      // Fatal errors, if occurred will be wrapped into a custom exception which will propagate to the bottom og the
      // run stack, and trigger the pipeline shutdown.
      try {
        whenEnoughResourcesAreAvailable(task.job.operation.consumeThreads) {
          log.warn(s"${Emoji.PARALLEL}Running task for the table: '${task.job.outputTable.name}' for '${task.infoDate}'...")
          runTask(task)
        }
      } catch {
        case NonFatal(ex) => throw ex
        case ex: Throwable => throw new FatalErrorWrapper("Fatal error has occurred.", ex)
      }
    })
  }

  override def runSequential(tasks: Seq[Task]): Future[Seq[RunStatus]] = {
    if (tasks.isEmpty) {
      Future.successful(Seq.empty[RunStatus])
    } else {
      Future {
        // We cannot use 'Try' here, need to use 'try' to catch fatal errors.
        // See the comment in runParallel(). Same reasoning.
        try {
          val requiredResources = tasks.map(_.job.operation.consumeThreads).max
          whenEnoughResourcesAreAvailable(requiredResources) {
            runDependentTasks(tasks)
          }
        } catch {
          case NonFatal(ex) => throw ex
          case ex: Throwable => throw new FatalErrorWrapper("Fatal error has occurred.", ex)
        }
      }
    }
  }

  override def close(): Unit = {
    executionContext.shutdown()
  }
}
