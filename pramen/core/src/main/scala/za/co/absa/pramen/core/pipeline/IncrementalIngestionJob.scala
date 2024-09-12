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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import za.co.absa.pramen.api.offset.OffsetValue
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

  override def validate(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config): Reason = {
    if (source.getOffsetInfo(sourceTable.query).nonEmpty) {
      Reason.Ready
    } else {
      Reason.NotReady(s"Offset column is not configured for source '$sourceName' of '${operationDef.name}'")
    }
  }

  override def run(infoDate: LocalDate, runReason: TaskRunReason, conf: Config): RunResult = {
    val columns = if (sourceTable.transformations.isEmpty && sourceTable.filters.isEmpty) {
      sourceTable.columns
    } else {
      Seq.empty[String]
    }

    val sourceResult = latestOffset match {
      case None =>
        source.getData(sourceTable.query, infoDate, infoDate, columns)
      case Some(maxOffset) =>
        if (runReason == TaskRunReason.Rerun)
          source.getIncrementalDataRange(sourceTable.query, maxOffset.minimumOffset, maxOffset.maximumOffset, Some(infoDate), columns)
        else
          source.getIncrementalData(sourceTable.query, maxOffset.maximumOffset, Some(infoDate), columns)
    }

    val sanitizedDf = sanitizeDfColumns(sourceResult.data, specialCharacters)

    val result = sourceResult.copy(data = sanitizedDf)

    RunResult(result.data, result.filesRead, result.warnings)
  }

  override def save(df: DataFrame,
                    infoDate: LocalDate,
                    runReason: TaskRunReason,
                    conf: Config,
                    jobStarted: Instant,
                    inputRecordCount: Option[Long]): SaveResult = {

    val dfToSave = df.withColumn(outputTable.batchIdColumn, lit(batchId))

    val om = bookkeeper.getOffsetManager

    val offsetInfo = source.getOffsetInfo(sourceTable.query).getOrElse(
      throw new IllegalArgumentException(s"Offset type is not configured for the source '$sourceName' outputting to '${outputTable.name}''")
    )

    val minimumOffset = latestOffset.map(_.maximumOffset).getOrElse(offsetInfo.minimalOffset)

    val req = om.startWriteOffsets(outputTable.name, infoDate, minimumOffset)

    val stats = try {
      val statsToReturn = metastore.saveTable(outputTable.name, infoDate, dfToSave, inputRecordCount, saveModeOverride = Some(SaveMode.Append))

      val updatedDf = metastore.getCurrentBatch(outputTable.name, infoDate)

      if (updatedDf.isEmpty) {
        om.rollbackOffsets(req)
      } else {
        val maxOffset = updatedDf.agg(max(col(offsetInfo.offsetColumn)).cast(StringType)).collect()(0)(0).asInstanceOf[String]
        om.commitOffsets(req, OffsetValue.fromString(offsetInfo.minimalOffset.dataTypeString, maxOffset))
      }
      statsToReturn
    } catch {
      case ex: Throwable =>
        om.rollbackOffsets(req)
        throw ex
    }

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
