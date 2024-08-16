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

package za.co.absa.pramen.core.tests.journal

import org.apache.avro.generic.GenericData.Array
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.PipelineInfo
import za.co.absa.pramen.api.status.{PipelineNotificationFailure, RunInfo, RunStatus, RuntimeInfo, TaskResult, TaskRunReason}
import za.co.absa.pramen.core.journal.model.TaskCompleted
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.mocks.job.JobSpy
import za.co.absa.pramen.core.pipeline.Task

import java.time.{Instant, LocalDate}

class TaskCompletedSuite extends AnyWordSpec {
  private val infoDate = LocalDate.of(2022, 1, 18)
  private val pipelineInfo = PipelineInfo(
    pipelineName = "test_pipeline",
    environment = "TEST",
    runtimeInfo = RuntimeInfo(),
    startedAt = Instant.now(),
    finishedAt = Some(Instant.now()),
    sparkApplicationId = Some("app_123"),
    failureException = None,
    pipelineNotificationFailures = Seq.empty,
    pipelineId = "test_pipeline_id",
    tenant = Some("test_tenant"))

  "fromTaskResult" should {
    "create a TaskCompleted from a TaskResult for a successful task" in {
      val now = Instant.now()
      val job = new JobSpy()
      val runReason = TaskRunReason.Update
      val task = Task(job, infoDate, runReason)
      val taskResult = TaskResult(
        job.name,
        MetaTable.getMetaTableDef(job.outputTable),
        RunStatus.Succeeded(Some(1000), 2000, Some(3000), runReason, Nil, Nil, Nil, Nil),
        Some(RunInfo(infoDate, now.minusSeconds(10), now)),
        "app_123",
        isTransient = false,
        isRawFilesJob = false,
        Nil,
        Nil,
        Nil,
        Map.empty)
      val taskCompleted = TaskCompleted.fromTaskResult(task, taskResult, pipelineInfo)

      assert(taskCompleted.jobName == job.name)
      assert(taskCompleted.tableName == job.outputTable.name)
      assert(taskCompleted.periodBegin == infoDate)
      assert(taskCompleted.periodEnd == infoDate)
      assert(taskCompleted.informationDate == infoDate)
      assert(taskCompleted.inputRecordCount == 2000)
      assert(taskCompleted.inputRecordCountOld == 1000)
      assert(taskCompleted.outputRecordCount.contains(2000))
      assert(taskCompleted.outputRecordCountOld.contains(1000))
      assert(taskCompleted.outputSize.contains(3000))
      assert(taskCompleted.startedAt == now.minusSeconds(10).getEpochSecond)
      assert(taskCompleted.finishedAt == now.getEpochSecond)
      assert(taskCompleted.status == "Update")
      assert(taskCompleted.sparkApplicationId == pipelineInfo.sparkApplicationId)
      assert(taskCompleted.pipelineId.contains(pipelineInfo.pipelineId))
      assert(taskCompleted.pipelineName.contains(pipelineInfo.pipelineName))
      assert(taskCompleted.environmentName.contains(pipelineInfo.environment))
      assert(taskCompleted.tenant == pipelineInfo.tenant)
    }

    "create a TaskCompleted from a TaskResult for a failed task" in {
      val now = Instant.now()
      val job = new JobSpy()
      val runReason = TaskRunReason.Update
      val task = Task(job, infoDate, runReason)
      val taskResult = TaskResult(
        job.name,
        MetaTable.getMetaTableDef(job.outputTable),
        RunStatus.Failed(new IllegalStateException("Dummy Exception")),
        None,
        "app_123",
        isTransient = false,
        isRawFilesJob = false,
        Nil,
        Nil,
        Nil,
        Map.empty)

      val taskCompleted = TaskCompleted.fromTaskResult(task, taskResult, pipelineInfo)

      assert(taskCompleted.jobName == job.name)
      assert(taskCompleted.tableName == job.outputTable.name)
      assert(taskCompleted.periodBegin == infoDate)
      assert(taskCompleted.periodEnd == infoDate)
      assert(taskCompleted.informationDate == infoDate)
      assert(taskCompleted.inputRecordCount == 0)
      assert(taskCompleted.inputRecordCountOld == 0)
      assert(taskCompleted.outputRecordCount.isEmpty)
      assert(taskCompleted.outputRecordCountOld.isEmpty)
      assert(taskCompleted.outputSize.isEmpty)
      assert(now.getEpochSecond - taskCompleted.startedAt < 1000)
      assert(now.getEpochSecond - taskCompleted.finishedAt < 1000)
      assert(taskCompleted.status == "Failed")
      assert(taskCompleted.failureReason.exists(_.contains("Dummy Exception")))
      assert(taskCompleted.sparkApplicationId == pipelineInfo.sparkApplicationId)
      assert(taskCompleted.pipelineId.contains(pipelineInfo.pipelineId))
      assert(taskCompleted.pipelineName.contains(pipelineInfo.pipelineName))
      assert(taskCompleted.environmentName.contains(pipelineInfo.environment))
      assert(taskCompleted.tenant == pipelineInfo.tenant)
    }
  }
}
