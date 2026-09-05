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

package za.co.absa.pramen.core.runner.repartitioner

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.status.RunStatus.Succeeded
import za.co.absa.pramen.api.status.{RunInfo, RunStatus, TaskDef, TaskResult}
import za.co.absa.pramen.api.status.TaskRunReason.OnRequest
import za.co.absa.pramen.bulkload.BulkLoadStateManager
import za.co.absa.pramen.bulkload.model.BulkLoadPhase
import za.co.absa.pramen.core.app.config.BulkRunConfig
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.metastore.peristence.MetastorePersistence
import za.co.absa.pramen.core.pipeline.Job
import za.co.absa.pramen.core.utils.{Emoji, TimeUtils}

import java.time.Instant
import scala.util.control.NonFatal

class JobRepartitionerImpl(bulkLoadCurrent: BulkRunConfig,
                           bulkLoadStateManager: BulkLoadStateManager,
                           metastore: Metastore,
                           appConfig: Config,
                           applicationId: String,
                           batchId: Long)(implicit spark: SparkSession) extends JobRepartitioner {
  private val log = LoggerFactory.getLogger(this.getClass)

  def repartition(job: Job): Seq[TaskResult] = {
    val start = Instant.now()
    try {
      doRepartition(job)
    } catch {
      case NonFatal(ex) =>
        val finish = Instant.now()
        log.info(s"${Emoji.FAILURE} The repartition job has FAILED (${job.taskDef.outputTable.name} for ${bulkLoadCurrent.dataDateFrom}..${bulkLoadCurrent.dataDateTo}). Elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")
        Seq(
          TaskResult(
            getRepartitionTaskDef(job),
            RunStatus.Failed(ex),
            Some(RunInfo(bulkLoadCurrent.outputInfoDate, start, finish)),
            applicationId,
            isTransient = false,
            isRawFilesJob = false,
            newSchemaRegistered = false,
            Seq.empty,
            Seq.empty,
            Seq.empty,
            Map.empty
          )
        )
    }
  }

  private[core] def doRepartition(job: Job): Seq[TaskResult] = {
    val outputTable = job.taskDef.outputTable.name
    val bulkLoadStateOpt = bulkLoadStateManager.getState(outputTable, bulkLoadCurrent.outputInfoDate)

    if (bulkLoadStateOpt.isEmpty) return Seq.empty
    val bulkLoadState = bulkLoadStateOpt.get

    if (bulkLoadCurrent.infoDateColumn.isEmpty) return Seq.empty
    val infoDateColumn = bulkLoadCurrent.infoDateColumn.get

    val metaTable = metastore.getTableDef(outputTable)
    val persistence = MetastorePersistence.fromMetaTable(metaTable, appConfig, batchId)

    val start = Instant.now()
    val secondStagePhase = if (bulkLoadState.phase == BulkLoadPhase.Processed) {
      // Starting repartitioning phase 1

      if (persistence.isRepartitioningSupported) {
        persistence.repartitionPhase1(infoDateColumn, bulkLoadCurrent.outputInfoDate, bulkLoadCurrent.outputInfoDate, bulkLoadCurrent.outputInfoDate)
        val updatedState = bulkLoadState.copy(phase = BulkLoadPhase.Repartition1)
        bulkLoadStateManager.updatePhase(updatedState)

        BulkLoadPhase.Repartition1
      } else {
        BulkLoadPhase.Done
      }
    } else {
      bulkLoadState.phase
    }

    val finalPhase = if (secondStagePhase == BulkLoadPhase.Repartition1) {
      // Starting repartitioning phase 2

      if (persistence.isRepartitioningSupported) {
        persistence.repartitionPhase2(infoDateColumn, bulkLoadCurrent.outputInfoDate, bulkLoadCurrent.outputInfoDate, bulkLoadCurrent.outputInfoDate)
        val updatedState = bulkLoadState.copy(phase = BulkLoadPhase.Done)
        bulkLoadStateManager.updatePhase(updatedState)
        BulkLoadPhase.Done
      } else {
        BulkLoadPhase.Done
      }
    } else {
      bulkLoadState.phase
    }

    val finish = Instant.now()

    if (finalPhase == BulkLoadPhase.Done) {
      log.info(s"${Emoji.SUCCESS} The repartition job has SUCCEEDED ($outputTable for ${bulkLoadCurrent.dataDateFrom}..${bulkLoadCurrent.dataDateTo}). Elapsed time: ${TimeUtils.getElapsedTimeStr(start, finish)}")
      val recordCount = metastore.getTable(outputTable, Some(bulkLoadCurrent.dataDateFrom), Some(bulkLoadCurrent.dataDateTo)).count()
      Seq(
        TaskResult(
          getRepartitionTaskDef(job),
          Succeeded(None, Some(recordCount), None, None, OnRequest, Seq.empty, Seq.empty, Seq.empty, Seq.empty),
          Some(RunInfo(bulkLoadCurrent.outputInfoDate, start, finish)),
          applicationId,
          isTransient = false,
          isRawFilesJob = false,
          newSchemaRegistered = false,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Map.empty
        )
      )

    } else {
      Seq.empty
    }
  }

  private[core] def getRepartitionTaskDef(job: Job): TaskDef = {
    val originalName = job.taskDef.name
    val newName = s"Repartition - $originalName"
    job.taskDef.copy(name = newName)
  }
}
