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
import za.co.absa.pramen.core.app.config.RuntimeConfig
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.pipeline.Task
import za.co.absa.pramen.core.state.PipelineState

import java.time.{Instant, LocalDate}
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext.fromExecutorService
import scala.concurrent.{ExecutionContextExecutorService, Future}

/**
  * The responsibility of this class is to handle the execution method.
  * This class runs tasks in multiple threads.
  */
class TaskRunnerMultithreaded(conf: Config,
                              bookkeeper: Bookkeeper,
                              pipelineState: PipelineState,
                              runtimeConfig: RuntimeConfig) extends TaskRunnerBase(conf, bookkeeper, runtimeConfig, pipelineState) {
  private val executor: ExecutorService = newFixedThreadPool(runtimeConfig.parallelTasks)
  implicit private val executionContext: ExecutionContextExecutorService = fromExecutorService(executor)

  def runParallel(tasks: Seq[Task]): Seq[Future[RunStatus]] = {
    tasks.map(task => Future {
      runTask(task)
    })
  }

  def runSequential(tasks: Seq[Task]): Future[Seq[RunStatus]] = {
    Future {
      runDependentTasks(tasks)
    }
  }

  def shutdown(): Unit = {
    executionContext.shutdown()
  }
}