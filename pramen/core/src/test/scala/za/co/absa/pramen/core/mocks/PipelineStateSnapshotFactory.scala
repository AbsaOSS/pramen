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

package za.co.absa.pramen.core.mocks

import za.co.absa.pramen.core.runner.task.{PipelineNotificationFailure, TaskResult}
import za.co.absa.pramen.core.state.PipelineStateSnapshot

object PipelineStateSnapshotFactory {
  def getDummyPipelineStateSnapshot(pipelineName: String = "TestApp",
                                    environmentName: String = "DEV",
                                    sparkAppId: Option[String] = Some("testid-12345"),
                                    isFinished: Boolean = false,
                                    exitedNormally: Boolean = false,
                                    exitCode: Int = 0,
                                    customShutdownHookCanRun: Boolean = false,
                                    failureException: Option[Throwable] = None,
                                    taskResults: Seq[TaskResult] = Seq.empty,
                                    pipelineNotificationFailures: Seq[PipelineNotificationFailure] = Seq.empty): PipelineStateSnapshot = {
    PipelineStateSnapshot(pipelineName,
      environmentName,
      sparkAppId,
      isFinished,
      exitedNormally,
      exitCode,
      customShutdownHookCanRun,
      failureException,
      taskResults,
      pipelineNotificationFailures
    )
  }
}
