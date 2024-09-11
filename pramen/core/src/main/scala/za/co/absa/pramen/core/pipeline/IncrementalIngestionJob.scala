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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import za.co.absa.pramen.api.status.{DependencyWarning, TaskRunReason}
import za.co.absa.pramen.api.{Query, Reason, Source, SourceResult}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.bookkeeper.model.DataOffsetAggregated
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.runner.splitter.{ScheduleStrategy, ScheduleStrategyIncremental}
import za.co.absa.pramen.core.utils.SparkUtils._

import java.time.{Instant, LocalDate}

class IncrementalIngestionJob(operationDef: OperationDef,
                              metastore: Metastore,
                              bookkeeper: Bookkeeper,
                              notificationTargets: Seq[JobNotificationTarget],
                              latestOffset: Option[DataOffsetAggregated],
                              batchId: Long,
                              sourceName: String,
                              source: Source,
                              sourceTable: SourceTable,
                              outputTable: MetaTable,
                              specialCharacters: String)
                             (implicit spark: SparkSession)
  extends IngestionJob(operationDef, metastore, bookkeeper, notificationTargets, sourceName, source, sourceTable, outputTable, specialCharacters, None, false) {

  override val scheduleStrategy: ScheduleStrategy = new ScheduleStrategyIncremental(latestOffset)

  override def trackDays: Int = 0

  override def preRunCheckJob(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    if (source.hasInfoDateColumn(sourceTable.query)) {
      super.preRunCheckJob(infoDate, runReason, jobConfig, dependencyWarnings)
    } else {
      latestOffset match {
        case Some(offset) =>
          if (offset.maximumInfoDate.isAfter(infoDate)) {
            JobPreRunResult(JobPreRunStatus.Skip("Retrospective runs are not allowed yet"), None, dependencyWarnings, Nil)
          } else {
            JobPreRunResult(JobPreRunStatus.Ready, None, dependencyWarnings, Nil)
          }
        case None =>
          JobPreRunResult(JobPreRunStatus.Ready, None, dependencyWarnings, Nil)
      }
    }
  }

  override def validate(infoDate: LocalDate, jobConfig: Config): Reason = {
    Reason.Ready
  }

  override def run(infoDate: LocalDate, conf: Config): RunResult = {
    val result = getSourcingResult(infoDate)

    RunResult(result.data, result.filesRead, result.warnings)
  }

  override def save(df: DataFrame,
                    infoDate: LocalDate,
                    conf: Config,
                    jobStarted: Instant,
                    inputRecordCount: Option[Long]): SaveResult = {

    val dfToSave = df.withColumn(outputTable.batchIdColumn, lit(batchId))

    val stats = metastore.saveTable(outputTable.name, infoDate, dfToSave, inputRecordCount, saveModeOverride = Some(SaveMode.Append))

    try {
      source.postProcess(
        sourceTable.query,
        outputTable.name,
        metastore.getMetastoreReader(Seq(outputTable.name), infoDate),
        infoDate,
        operationDef.extraOptions
      )
    } catch {
      case _: AbstractMethodError => log.warn(s"Sources were built using old version of Pramen that does not support post processing. Ignoring...")
    }

    source.close()

    val jobFinished = Instant.now
    val tooLongWarnings = getTookTooLongWarnings(jobStarted, jobFinished, sourceTable.warnMaxExecutionTimeSeconds)

    SaveResult(stats, warnings = tooLongWarnings)
  }

  private def getSourcingResult(infoDate: LocalDate): SourceResult = {
    val (from, to) = getInfoDateRange(infoDate, sourceTable.rangeFromExpr, sourceTable.rangeToExpr)

    getData(source, sourceTable.query, from, to)
  }

  private def getData(source: Source, query: Query, from: LocalDate, to: LocalDate): SourceResult = {
    val sourceResult = if (sourceTable.transformations.isEmpty && sourceTable.filters.isEmpty)
      source.getData(query, from, to, sourceTable.columns) // push down the projection
    else
      source.getData(query, from, to, Seq.empty[String]) // column selection and order will be applied later

    val sanitizedDf = sanitizeDfColumns(sourceResult.data, specialCharacters)

    sourceResult.copy(data = sanitizedDf)
  }
}
