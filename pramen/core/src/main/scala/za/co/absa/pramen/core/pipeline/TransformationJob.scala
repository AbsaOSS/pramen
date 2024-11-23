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

package za.co.absa.pramen.core.pipeline

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import za.co.absa.pramen.api.status.{DependencyWarning, JobType, TaskRunReason}
import za.co.absa.pramen.api.{Reason, Transformer}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.metastore.{Metastore, MetastoreReaderCore}
import za.co.absa.pramen.core.runner.splitter.{ScheduleStrategy, ScheduleStrategyIncremental, ScheduleStrategySourcing}

import java.time.{Instant, LocalDate}

class TransformationJob(operationDef: OperationDef,
                        metastore: Metastore,
                        bookkeeper: Bookkeeper,
                        notificationTargets: Seq[JobNotificationTarget],
                        outputTable: MetaTable,
                        transformerFactoryClass: String,
                        transformer: Transformer,
                        batchId: Long)
                       (implicit spark: SparkSession)
  extends JobBase(operationDef, metastore, bookkeeper, notificationTargets, outputTable) {

  override val jobType: JobType = JobType.Transformation(transformerFactoryClass)

  private val inputTables = operationDef.dependencies.flatMap(_.tables).distinct

  override val scheduleStrategy: ScheduleStrategy = {
    if (isIncremental)
      new ScheduleStrategyIncremental(None, true)
    else
      new ScheduleStrategySourcing
  }

  override def preRunCheckJob(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    preRunTransformationCheck(infoDate, runReason, dependencyWarnings)
  }

  override def validate(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config): Reason = {
    transformer.validate(metastore.getMetastoreReader(inputTables, outputTable.name, infoDate, runReason, isIncremental, incrementalDryRun = true, isPostProcessing = false), infoDate, operationDef.extraOptions)
  }

  override def run(infoDate: LocalDate, runReason: TaskRunReason, conf: Config): RunResult = {
    val isTransitive = outputTable.format.isTransient
    val metastoreReader = metastore.getMetastoreReader(inputTables, outputTable.name, infoDate, runReason, isIncremental, incrementalDryRun = false, isPostProcessing = !isTransitive)
    val runResult = try {
      RunResult(transformer.run(metastoreReader, infoDate, operationDef.extraOptions))
    } finally {
      metastoreReader.asInstanceOf[MetastoreReaderCore].commitIncremental()
    }

    runResult
  }

  def postProcessing(df: DataFrame,
                     infoDate: LocalDate,
                     conf: Config): DataFrame = {
    df
  }

  override def save(df: DataFrame,
                    infoDate: LocalDate,
                    runReason: TaskRunReason,
                    conf: Config,
                    jobStarted: Instant,
                    inputRecordCount: Option[Long]): SaveResult = {
    val saveResults = if (isIncremental && runReason != TaskRunReason.Rerun)
      SaveResult(metastore.saveTable(outputTable.name, infoDate, df, None, saveModeOverride = Some(SaveMode.Append)))
    else
      SaveResult(metastore.saveTable(outputTable.name, infoDate, df, None))

    val metastoreReader = metastore.getMetastoreReader(inputTables :+ outputTable.name, outputTable.name, infoDate, runReason, isIncremental, incrementalDryRun = false, isPostProcessing = true)

    try {
      transformer.postProcess(
        outputTable.name,
        metastoreReader,
        infoDate, operationDef.extraOptions
      )
    } catch {
      case _: AbstractMethodError => log.warn(s"Transformers were built using old version of Pramen that does not support post processing. Ignoring...")
    }

    val jobFinished = Instant.now
    val tooLongWarnings = getTookTooLongWarnings(jobStarted, jobFinished, None)

    saveResults.copy(warnings = saveResults.warnings ++ tooLongWarnings)
  }
}
