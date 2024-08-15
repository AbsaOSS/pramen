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

package za.co.absa.pramen.core.mocks.state

import za.co.absa.pramen.api.status.{PipelineStateSnapshot, TaskResult}
import za.co.absa.pramen.core.mocks.{PipelineInfoFactory, PipelineStateSnapshotFactory}
import za.co.absa.pramen.core.state.PipelineState

import scala.collection.mutable.ListBuffer

class PipelineStateSpy extends PipelineState {
  var setShutdownHookCanRunCount = 0
  var setSuccessCount = 0
  var setWarningCount = 0
  var getExitCodeCalled = 0
  var jobFailureCalled = 0
  val failures = new ListBuffer[(String, Throwable)]
  val completedStatuses = new ListBuffer[TaskResult]
  var closeCalled = 0
  var sparkAppId: Option[String] = None

  override def getState(): PipelineStateSnapshot = {
    PipelineStateSnapshotFactory.getDummyPipelineStateSnapshot(PipelineInfoFactory.getDummyPipelineInfo(sparkApplicationId = sparkAppId),
      customShutdownHookCanRun = setShutdownHookCanRunCount > 0,
      taskResults = completedStatuses.toList
    )
  }

  override def setShutdownHookCanRun(): Unit = synchronized {
    setShutdownHookCanRunCount += 1
  }

  override def setSuccess(): Unit = synchronized {
    setSuccessCount += 1
  }

  override def setWarningFlag(): Unit = synchronized {
    setWarningCount += 1
  }

  override def setFailure(stage: String, ex: Throwable): Unit = synchronized {
    failures.append((stage, ex))
  }

  override def setSparkAppId(sparkAppId: String): Unit = synchronized {
    this.sparkAppId = Option(sparkAppId)
  }

  override def addTaskCompletion(statuses: Seq[TaskResult]): Unit = synchronized {
    completedStatuses ++= statuses
  }

  override def getExitCode: Int = synchronized {
    getExitCodeCalled += 1
    0
  }

  override def close(): Unit = closeCalled += 1
}
