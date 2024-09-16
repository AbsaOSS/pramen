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
import org.apache.spark.sql.types.{IntegerType, LongType, ShortType, StringType}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import za.co.absa.pramen.api.offset.{DataOffset, OffsetInfo, OffsetValue}
import za.co.absa.pramen.api.status.{DependencyWarning, TaskRunReason}
import za.co.absa.pramen.api.{Reason, Source}
import za.co.absa.pramen.core.bookkeeper.model.{DataOffsetAggregated, DataOffsetRequest}
import za.co.absa.pramen.core.bookkeeper.{Bookkeeper, OffsetManager}
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.runner.splitter.{ScheduleStrategy, ScheduleStrategyIncremental}
import za.co.absa.pramen.core.utils.SparkUtils._

import java.time.{Instant, LocalDate}

class IncrementalIngestionJob(operationDef: OperationDef,
                              metastore: Metastore,
                              bookkeeper: Bookkeeper,
                              notificationTargets: Seq[JobNotificationTarget],
                              latestOffsetIn: Option[DataOffsetAggregated],
                              batchId: Long,
                              sourceName: String,
                              source: Source,
                              sourceTable: SourceTable,
                              outputTable: MetaTable,
                              specialCharacters: String)
                             (implicit spark: SparkSession)
  extends IngestionJob(operationDef, metastore, bookkeeper, notificationTargets, sourceName, source, sourceTable, outputTable, specialCharacters, None, false) {

  private var latestOffset = latestOffsetIn

  override val scheduleStrategy: ScheduleStrategy = new ScheduleStrategyIncremental(latestOffset, source.hasInfoDateColumn(sourceTable.query))

  override def trackDays: Int = 0

  override def preRunCheckJob(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    val om = bookkeeper.getOffsetManager

    val uncommittedOffsets = om.getOffsets(outputTable.name, infoDate).filter(_.committedAt.isEmpty)

    if (uncommittedOffsets.nonEmpty) {
      log.warn(s"Found uncommitted offsets for ${outputTable.name} at $infoDate. Fixing...")
      handleUncommittedOffsets(om, metastore, infoDate, uncommittedOffsets)
    }

    val hasInfoDateColumn = source.hasInfoDateColumn(sourceTable.query)
    if (hasInfoDateColumn && runReason == TaskRunReason.Rerun) {
      super.preRunCheckJob(infoDate, runReason, jobConfig, dependencyWarnings)
    } else {
      JobPreRunResult(JobPreRunStatus.Ready, None, dependencyWarnings, Nil)
    }
  }

  private def handleUncommittedOffsets(om: OffsetManager, mt: Metastore, infoDate: LocalDate, uncommittedOffsets: Array[DataOffset]): Unit = {
    val minOffset = uncommittedOffsets.map(_.minOffset).min

    val offsetInfo = source.getOffsetInfo(sourceTable.query).getOrElse(throw new IllegalArgumentException(s"Offset column not defined for the ingestion job '${operationDef.name}', " +
      s"query: '${sourceTable.query.query}''"))

    val df = try {
      mt.getTable(outputTable.name, Option(infoDate), Option(infoDate))
    } catch {
      case ex: AnalysisException =>
        log.warn(s"No data found for ${outputTable.name}. Rolling back uncommitted offsets...", ex)

        uncommittedOffsets.foreach { of =>
          log.warn(s"Cleaning uncommitted offset: $of...")
          om.rollbackOffsets(DataOffsetRequest(outputTable.name, infoDate, of.minOffset, of.createdAt))
        }

        latestOffset = om.getMaxInfoDateAndOffset(outputTable.name, None)
        return
    }

    if (!df.schema.fields.exists(_.name.equalsIgnoreCase(offsetInfo.offsetColumn))) {
      throw new IllegalArgumentException(s"Offset column '${offsetInfo.offsetColumn}' not found in the output table '${outputTable.name}'. Cannot update uncommitted offsets.")
    }

    val newMaxOffset = if (df.isEmpty) {
      minOffset
    } else {
      val row = df.agg(max(col(offsetInfo.offsetColumn)).cast(StringType)).collect()(0)
      OffsetValue.fromString(offsetInfo.minimalOffset.dataTypeString, row(0).asInstanceOf[String])
    }

    log.warn(s"Fixing uncommitted offsets. New offset to commit for ${outputTable.name} at $infoDate: " +
      s"min offset: ${minOffset.valueString}, max offset: ${newMaxOffset.valueString}.")

    val req = om.startWriteOffsets(outputTable.name, infoDate, minOffset)
    om.commitOffsets(req, newMaxOffset)

    uncommittedOffsets.foreach { of =>
      log.warn(s"Cleaning uncommitted offset: $of...")
      om.rollbackOffsets(DataOffsetRequest(outputTable.name, infoDate, of.minOffset, of.createdAt))
    }

    latestOffset = om.getMaxInfoDateAndOffset(outputTable.name, None)
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
    val isRerun = runReason == TaskRunReason.Rerun

    val dfToSave = df.withColumn(outputTable.batchIdColumn, lit(batchId))

    val om = bookkeeper.getOffsetManager

    val offsetInfo = source.getOffsetInfo(sourceTable.query).getOrElse(
      throw new IllegalArgumentException(s"Offset type is not configured for the source '$sourceName' outputting to '${outputTable.name}''")
    )

    validateOffsetColumn(df, offsetInfo)

    val minimumOffset = latestOffset.map(_.maximumOffset).getOrElse(offsetInfo.minimalOffset)

    val req = om.startWriteOffsets(outputTable.name, infoDate, minimumOffset)

    val stats = try {
      val statsToReturn = if (isRerun) {
        metastore.saveTable(outputTable.name, infoDate, dfToSave, inputRecordCount, saveModeOverride = Some(SaveMode.Overwrite))
      } else {
        metastore.saveTable(outputTable.name, infoDate, dfToSave, inputRecordCount, saveModeOverride = Some(SaveMode.Append))
      }

      val updatedDf = metastore.getCurrentBatch(outputTable.name, infoDate)

      if (updatedDf.isEmpty) {
        if (isRerun) {
          om.commitRerun(req, OffsetValue.getMinimumForType(offsetInfo.minimalOffset.dataTypeString))
        } else {
          om.rollbackOffsets(req)
        }
      } else {
        val row = updatedDf.agg(max(col(offsetInfo.offsetColumn)).cast(StringType)).collect()(0)
        val maxOffset = OffsetValue.fromString(offsetInfo.minimalOffset.dataTypeString, row(0).asInstanceOf[String])

        if (isRerun) {
          om.commitRerun(req, maxOffset)
        } else {
          om.commitOffsets(req, maxOffset)
        }
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
        metastore.getMetastoreReader(Seq(outputTable.name), infoDate, runReason, isIncremental = true),
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

  private[core] def validateOffsetColumn(df: DataFrame, offsetInfo: OffsetInfo): Unit = {
    if (!df.schema.fields.exists(_.name.equalsIgnoreCase(offsetInfo.offsetColumn))) {
      throw new IllegalArgumentException(s"Offset column '${offsetInfo.offsetColumn}' not found in the output table '${outputTable.name}'.")
    }

    val field = df.schema.fields.find(_.name.equalsIgnoreCase(offsetInfo.offsetColumn)).get

    offsetInfo.minimalOffset match {
      case v: OffsetValue.LongType =>
        if (!field.dataType.isInstanceOf[ShortType] && !field.dataType.isInstanceOf[IntegerType] && !field.dataType.isInstanceOf[LongType]) {
          throw new IllegalArgumentException(s"Offset column '${offsetInfo.offsetColumn}' has type '${field.dataType}'. " +
            s"But only integral types are supported for offset type '${v.dataTypeString}'.")
        }
      case v: OffsetValue.StringType =>
        if (!field.dataType.isInstanceOf[StringType]) {
          throw new IllegalArgumentException(s"Offset column '${offsetInfo.offsetColumn}' has type '${field.dataType}'. " +
            s"But only string type is supported for offset type '${v.dataTypeString}'.")
        }
    }
  }
}
