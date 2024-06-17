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

import za.co.absa.pramen.api.status.RunStatus
import za.co.absa.pramen.core.pipeline.{Job, TaskPreDef}

import java.time.LocalDate
import scala.concurrent.Future

trait TaskRunner extends AutoCloseable {
  /** Run a job for specified information dates as a part of pipeline execution. */
  def runJobTasks(job: Job, infoDates: Seq[TaskPreDef]): Future[Seq[RunStatus]]

  /** Run a job for specified information date when requested from another job. */
  def runLazyTask(job: Job, infoDate: LocalDate): RunStatus
}
