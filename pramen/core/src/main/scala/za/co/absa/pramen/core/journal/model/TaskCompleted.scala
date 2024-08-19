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

package za.co.absa.pramen.core.journal.model

import za.co.absa.pramen.api.PipelineInfo
import za.co.absa.pramen.api.status.RunStatus.Succeeded
import za.co.absa.pramen.api.status.TaskResult
import za.co.absa.pramen.core.pipeline.Task

import java.time.{Instant, LocalDate}

case class TaskCompleted(
                          jobName: String,
                          tableName: String,
                          periodBegin: LocalDate,
                          periodEnd: LocalDate,
                          informationDate: LocalDate,
                          inputRecordCount: Long,
                          inputRecordCountOld: Long,
                          outputRecordCount: Option[Long],
                          outputRecordCountOld: Option[Long],
                          outputSize: Option[Long],
                          startedAt: Long,
                          finishedAt: Long,
                          status: String,
                          failureReason: Option[String],
                          sparkApplicationId: Option[String],
                          pipelineId: Option[String],
                          pipelineName: Option[String],
                          environmentName: Option[String],
                          tenant: Option[String]
                        )

object TaskCompleted {
  /**
    * TaskCompleted is a legacy form of a task completion status. This is added for compatibility.
    * The journal might be eventually deprecated.
    */
  def fromTaskResult(task: Task, taskResult: TaskResult, pipelineInfo: PipelineInfo): TaskCompleted = {
    val now = Instant.now().getEpochSecond
    val taskStarted = taskResult.runInfo.map(_.started.getEpochSecond).getOrElse(now)
    val taskFinished = taskResult.runInfo.map(_.finished.getEpochSecond).getOrElse(now)
    val status = taskResult.runStatus.toString
    val failureReason = taskResult.runStatus.getReason
    val sparkApplicationId = Option(taskResult.applicationId)

    val (recordCountOld, inputRecordCount, outputRecordCount, sizeBytes) = taskResult.runStatus match {
      case s: Succeeded => (s.recordCountOld, s.recordCount, Some(s.recordCount), s.sizeBytes)
      case _            => (None, 0L, None, None)
    }

    TaskCompleted(
      task.job.name,
      task.job.outputTable.name,
      task.infoDate,
      task.infoDate,
      task.infoDate,
      inputRecordCount,
      recordCountOld.getOrElse(0L),
      outputRecordCount,
      recordCountOld,
      sizeBytes,
      taskStarted,
      taskFinished,
      status,
      failureReason,
      sparkApplicationId,
      Option(pipelineInfo.pipelineId),
      Option(pipelineInfo.pipelineName),
      Option(pipelineInfo.environment),
      pipelineInfo.tenant
    )
  }
}
