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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import za.co.absa.pramen.api.offset.DataOffset.UncommittedOffset
import za.co.absa.pramen.api.offset.{OffsetInfo, OffsetType, OffsetValue}
import za.co.absa.pramen.api.sql.SqlGeneratorBase
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
    if (source.getOffsetInfo.isEmpty) {
      return throw new IllegalArgumentException(s"Offset column is not configured for source '$sourceName' of '${operationDef.name}'")
    }

    val om = bookkeeper.getOffsetManager
    latestOffset = om.getMaxInfoDateAndOffset(outputTable.name, None)

    val onlyForInfoDate = if (source.hasInfoDateColumn(sourceTable.query))
      Some(infoDate)
    else
      None

    val uncommittedOffsets = om.getUncommittedOffsets(outputTable.name, onlyForInfoDate)

    if (uncommittedOffsets.nonEmpty) {
      if (onlyForInfoDate.isEmpty) {
        log.warn(s"Found uncommitted offsets for ${outputTable.name} at $infoDate. Fixing...")
      } else {
        log.warn(s"Found uncommitted offsets for ${outputTable.name}. Fixing...")
      }

      handleUncommittedOffsets(om, metastore, uncommittedOffsets)
    }

    JobPreRunResult(JobPreRunStatus.Ready, None, dependencyWarnings, Nil)
  }

  override def validate(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config): Reason = {
    val hasInfoDate = source.hasInfoDateColumn(sourceTable.query)
    val isReRun = runReason == TaskRunReason.Rerun

    if (source.getOffsetInfo.isEmpty) {
      return Reason.NotReady(s"Offset column is not configured for source '$sourceName' of '${operationDef.name}'")
    }

    (hasInfoDate, isReRun) match {
      case (false, false) =>
        latestOffset match {
          case Some(offset) if offset.maximumInfoDate.isAfter(infoDate) =>
            log.warn(s"Cannot run '${outputTable.name}' for '$infoDate' since offsets exists for ${offset.maximumInfoDate}.")
            Reason.Skip("Incremental ingestion cannot be retrospective")
          case _ =>
            Reason.Ready
        }
      case (false, true) =>
        val om = bookkeeper.getOffsetManager

        om.getMaxInfoDateAndOffset(outputTable.name, Option(infoDate)) match {
          case Some(offsets) =>
            log.info(s"Rerunning ingestion to '${outputTable.name}' at '$infoDate' for ${offsets.minimumOffset.valueString} < offsets <= ${offsets.maximumOffset.valueString}.")
            Reason.Ready
          case None =>
            log.info(s"Offsets not found for '${outputTable.name}' at '$infoDate'.")
            Reason.SkipOnce("No offsets registered")
        }
      case (true, false) =>
        Reason.Ready
      case (true, true) =>
        log.info(s"Rerunning ingestion to '${outputTable.name}' at '$infoDate'.")
        Reason.Ready
    }
  }

  override def run(infoDate: LocalDate, runReason: TaskRunReason, conf: Config): RunResult = {
    val columns = if (sourceTable.transformations.isEmpty && sourceTable.filters.isEmpty) {
      sourceTable.columns
    } else {
      Seq.empty[String]
    }

    val hasInfoDate = source.hasInfoDateColumn(sourceTable.query)
    val isReRun = runReason == TaskRunReason.Rerun

    val sourceResult = (hasInfoDate, isReRun) match {
      case (false, false) =>
        latestOffset match {
          case Some(maxOffset) =>
            source.getDataIncremental(sourceTable.query, None, Option(maxOffset.maximumOffset), None, columns)
          case None =>
            source.getData(sourceTable.query, infoDate, infoDate, columns)
        }
      case (false, true) =>
        val om = bookkeeper.getOffsetManager

        om.getMaxInfoDateAndOffset(outputTable.name, Option(infoDate)) match {
          case Some(offsets) =>
            source.getDataIncremental(sourceTable.query, None, Option(offsets.minimumOffset), Option(offsets.maximumOffset), columns)
          case None =>
            throw new IllegalStateException(s"No offsets for '${outputTable.name}' for '$infoDate'. Cannot rerun.")
        }
      case (true, false) =>
        val om = bookkeeper.getOffsetManager
        val infoDateLatestOffset = om.getMaxInfoDateAndOffset(outputTable.name, Some(infoDate))
        infoDateLatestOffset match {
          case Some(maxOffset) =>
            log.info(s"Running ingestion to '${outputTable.name}' at '$infoDate' for offset > ${maxOffset.maximumOffset.valueString}.")
            source.getDataIncremental(sourceTable.query, Option(infoDate), Option(maxOffset.maximumOffset), None, columns)
          case None =>
            log.info(s"Running ingestion to '${outputTable.name}' at '$infoDate' for all data available at the day.")
            source.getData(sourceTable.query, infoDate, infoDate, columns)
        }
      case (true, true) =>
        source.getData(sourceTable.query, infoDate, infoDate, columns)
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

    val offsetInfo = source.getOffsetInfo.getOrElse(
      throw new IllegalArgumentException(s"Offset type is not configured for the source '$sourceName' outputting to '${outputTable.name}''")
    )

    validateOffsetColumn(df, offsetInfo)

    val req = om.startWriteOffsets(outputTable.name, infoDate, offsetInfo.offsetType)

    val stats = try {
      val statsToReturn = if (isRerun) {
        metastore.saveTable(outputTable.name, infoDate, dfToSave, inputRecordCount, saveModeOverride = Some(SaveMode.Overwrite))
      } else {
        metastore.saveTable(outputTable.name, infoDate, dfToSave, inputRecordCount, saveModeOverride = Some(SaveMode.Append))
      }

      val updatedDf = metastore.getBatch(outputTable.name, infoDate, None)

      if (updatedDf.isEmpty) {
        om.rollbackOffsets(req)
      } else {
        val (minOffset, maxOffset) = getMinMaxOffsetFromDf(df, offsetInfo)

        if (isRerun) {
          om.commitRerun(req, minOffset, maxOffset)
        } else {
          om.commitOffsets(req, minOffset, maxOffset)
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

  private[core] def handleUncommittedOffsets(om: OffsetManager, mt: Metastore, uncommittedOffsets: Array[UncommittedOffset]): Unit = {
    import za.co.absa.pramen.core.utils.DateUtils._

    val offsetInfo = source.getOffsetInfo.getOrElse(throw new IllegalArgumentException(s"Offset column not defined for the ingestion job '${operationDef.name}', " +
      s"query: '${sourceTable.query.query}''"))

    val infoDates = uncommittedOffsets.map(_.infoDate).distinct.sorted

    infoDates.foreach { infoDate =>
      handleUncommittedOffsetsForDay(om, mt, uncommittedOffsets.filter(_.infoDate == infoDate), infoDate, offsetInfo)
    }
  }

  private[core] def handleUncommittedOffsetsForDay(om: OffsetManager, mt: Metastore, uncommittedOffsets: Array[UncommittedOffset], infoDate: LocalDate, offsetInfo: OffsetInfo): Unit = {
    val df = try {
      mt.getTable(outputTable.name, Option(infoDate), Option(infoDate))
    } catch {
      case _: AnalysisException =>
        log.warn(s"Table ${outputTable.name} has empty partition for $infoDate. Rolling back uncommitted offsets..")
        rollbackOffsets(infoDate, om, uncommittedOffsets)
        return
    }

    if (df.isEmpty) {
      log.warn(s"Table ${outputTable.name} is empty for $infoDate. Rolling back uncommitted offsets...")
      rollbackOffsets(infoDate, om, uncommittedOffsets)
      return
    }

    if (!df.schema.fields.exists(_.name.equalsIgnoreCase(offsetInfo.offsetColumn))) {
      throw new IllegalArgumentException(s"Offset column '${offsetInfo.offsetColumn}' not found in the output table '${outputTable.name}'. Cannot update uncommitted offsets.")
    }

    val (newMinOffset, newMaxOffset) = getMinMaxOffsetFromDf(df, offsetInfo)

    log.warn(s"Fixing uncommitted offsets. New offset to commit for ${outputTable.name} at $infoDate: " +
      s"min offset: ${newMinOffset.valueString}, max offset: ${newMaxOffset.valueString}.")

    val req = om.startWriteOffsets(outputTable.name, infoDate, offsetInfo.offsetType)
    om.commitOffsets(req, newMinOffset, newMaxOffset)

    uncommittedOffsets.foreach { of =>
      log.warn(s"Cleaning uncommitted offset: $of...")
      om.rollbackOffsets(DataOffsetRequest(outputTable.name, infoDate, of.batchId, of.createdAt))
    }

    latestOffset = om.getMaxInfoDateAndOffset(outputTable.name, None)
  }

  private[core] def rollbackOffsets(infoDate: LocalDate, om: OffsetManager, uncommittedOffsets: Array[UncommittedOffset]): Unit = {
    log.warn(s"No data found for ${outputTable.name}. Rolling back uncommitted offsets...")

    uncommittedOffsets.foreach { of =>
      log.warn(s"Cleaning uncommitted offset: $of...")
      om.rollbackOffsets(DataOffsetRequest(outputTable.name, infoDate, of.batchId, of.createdAt))
    }

    latestOffset = om.getMaxInfoDateAndOffset(outputTable.name, None)
  }

  private[core] def getMinMaxOffsetFromDf(df: DataFrame, offsetInfo: OffsetInfo): (OffsetValue, OffsetValue) = {
    val row = df.agg(min(offsetInfo.offsetType.getSparkCol(col(offsetInfo.offsetColumn)).cast(StringType)),
        max(offsetInfo.offsetType.getSparkCol(col(offsetInfo.offsetColumn))).cast(StringType))
      .collect()(0)
    val minValue = OffsetValue.fromString(offsetInfo.offsetType.dataTypeString, row(0).asInstanceOf[String]).get
    val maxValue = OffsetValue.fromString(offsetInfo.offsetType.dataTypeString, row(1).asInstanceOf[String]).get

    SqlGeneratorBase.validateOffsetValue(minValue)
    SqlGeneratorBase.validateOffsetValue(maxValue)

    (minValue, maxValue)
  }

  private[core] def validateOffsetColumn(df: DataFrame, offsetInfo: OffsetInfo): Unit = {
    if (!df.schema.fields.exists(_.name.equalsIgnoreCase(offsetInfo.offsetColumn))) {
      throw new IllegalArgumentException(s"Offset column '${offsetInfo.offsetColumn}' not found in the output table '${outputTable.name}'.")
    }

    val field = df.schema.fields.find(_.name.equalsIgnoreCase(offsetInfo.offsetColumn)).get

    offsetInfo.offsetType match {
      case OffsetType.DateTimeType =>
        if (!field.dataType.isInstanceOf[TimestampType]) {
          throw new IllegalArgumentException(s"Offset column '${offsetInfo.offsetColumn}' has type '${field.dataType}'. " +
            s"But only '${TimestampType.typeName}' is supported for offset type '${offsetInfo.offsetType.dataTypeString}'.")
        }
      case OffsetType.IntegralType =>
        if (!field.dataType.isInstanceOf[ShortType] && !field.dataType.isInstanceOf[IntegerType] && !field.dataType.isInstanceOf[LongType]) {
          throw new IllegalArgumentException(s"Offset column '${offsetInfo.offsetColumn}' has type '${field.dataType}'. " +
            s"But only integral types are supported for offset type '${offsetInfo.offsetType.dataTypeString}'.")
        }
      case OffsetType.StringType =>
        if (!field.dataType.isInstanceOf[StringType]) {
          throw new IllegalArgumentException(s"Offset column '${offsetInfo.offsetColumn}' has type '${field.dataType}'. " +
            s"But only string type is supported for offset type '${offsetInfo.offsetType.dataTypeString}'.")
        }
    }
  }
}
