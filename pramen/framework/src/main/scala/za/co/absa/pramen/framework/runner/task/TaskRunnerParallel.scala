/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.runner.task

import com.typesafe.config.Config
import za.co.absa.pramen.framework.app.config.RuntimeConfig
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.job.v2.job.Task
import za.co.absa.pramen.framework.state.PipelineState

import java.time.Instant
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext.fromExecutorService
import scala.concurrent.{ExecutionContextExecutorService, Future}

class TaskRunnerParallel(conf: Config,
                         bookkeeper: SyncBookKeeper,
                         pipelineState: PipelineState,
                         runtimeConfig: RuntimeConfig) extends TaskRunnerBase(conf, bookkeeper, runtimeConfig) {
  private val executor: ExecutorService = newFixedThreadPool(runtimeConfig.parallelTasks)
  implicit private val executionContext: ExecutionContextExecutorService = fromExecutorService(executor)

  def runAllTasks(tasks: Seq[Task]): Seq[Future[RunStatus]] = {
    tasks.map(task => Future {
      val started = Instant.now()

      val result = validate(task, started) match {
        case Left(failedResult)         => failedResult
        case Right(validationResult) => run(task, started, validationResult)
      }
      logTaskResult(result)
      pipelineState.addTaskCompletion(Seq(result))
      result.runStatus
    })
  }

  def shutdown(): Unit = {
    executionContext.shutdown()
  }
}
