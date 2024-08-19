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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.status.{DependencyWarning, TaskRunReason}
import za.co.absa.pramen.api.{Query, Reason, Source, SourceResult}
import za.co.absa.pramen.core.app.config.GeneralConfig.TEMPORARY_DIRECTORY_KEY
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.metastore.peristence.TransientTableManager
import za.co.absa.pramen.core.runner.splitter.{ScheduleStrategy, ScheduleStrategySourcing}
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.Emoji.WARNING
import za.co.absa.pramen.core.utils.SparkUtils._

import java.time.{Instant, LocalDate}

class IngestionJob(operationDef: OperationDef,
                   metastore: Metastore,
                   bookkeeper: Bookkeeper,
                   notificationTargets: Seq[JobNotificationTarget],
                   sourceName: String,
                   source: Source,
                   sourceTable: SourceTable,
                   outputTable: MetaTable,
                   specialCharacters: String,
                   tempDirectory: Option[String],
                   disableCountQuery: Boolean)
                  (implicit spark: SparkSession)
  extends JobBase(operationDef, metastore, bookkeeper, notificationTargets, outputTable) {
  import JobBase._

  override val scheduleStrategy: ScheduleStrategy = new ScheduleStrategySourcing

  override def trackDays: Int = {
    val hasInfoDate = try {
      source.hasInfoDateColumn(sourceTable.query)
    } catch {
      case _: AbstractMethodError =>
        log.warn(s"Sources were built using old version of Pramen that does not support track days handling for snapshot tables. Ignoring...")
        true
    }

    if (supportsTrackDays(hasInfoDate))
      outputTable.trackDays
    else
      0
  }

  override def preRunCheckJob(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    source.connect()

    val minimumRecordsOpt = ConfigUtils.getOptionInt(source.config, MINIMUM_RECORDS_KEY)
    val failIfNoAnyData = ConfigUtils.getOptionBoolean(source.config, FAIL_NO_DATA_KEY).getOrElse(false)
    val failIfNoNewData = ConfigUtils.getOptionBoolean(source.config, FAIL_NO_NEW_DATA_KEY).getOrElse(false)
    val failIfNoLateData = ConfigUtils.getOptionBoolean(source.config, FAIL_NO_LATE_DATA_KEY).getOrElse(false)

    val failIfNoData = if (runReason == TaskRunReason.Late) {
      failIfNoAnyData || failIfNoLateData
    } else {
      failIfNoAnyData || failIfNoNewData
    }

    val dataChunk = bookkeeper.getLatestDataChunk(sourceTable.metaTableName, infoDate, infoDate)

    val (from, to) = getInfoDateRange(infoDate, sourceTable.rangeFromExpr, sourceTable.rangeToExpr)

    val validationResult = source.validate(sourceTable.query, from, to)

    val warnings = validationResult match {
      case Reason.Ready =>
        log.info(s"Validation of '${outputTable.name}' for $from..$to has succeeded.")
        Seq.empty[String]
      case Reason.Warning(warnings) =>
        log.warn(s"Validation of '${outputTable.name}' for $from..$to returned warnings: ${warnings.mkString("\n")}.")
        warnings
      case Reason.NotReady(msg) =>
        log.info(s"failIfNoAnyData=$failIfNoAnyData, failIfNoLateData=$failIfNoLateData, failIfNoNewData=$failIfNoNewData, failIfNoData=$failIfNoData")
        log.warn(s"Validation of '${outputTable.name}' for $from..$to returned 'not ready'. Reason: $msg. ")
        return JobPreRunResult(JobPreRunStatus.NoData(failIfNoData), None, dependencyWarnings, Seq(msg))
      case Reason.Skip(msg) =>
        log.warn(s"Validation of '${outputTable.name}' for $from..$to requested to skip the task. Reason: $msg.")
        return JobPreRunResult(JobPreRunStatus.Skip(msg), None, dependencyWarnings, Nil)
      case Reason.SkipOnce(msg) =>
        log.warn(s"Validation of '${outputTable.name}' for $from..$to requested to skip the task. Reason: $msg.")
        return JobPreRunResult(JobPreRunStatus.Skip(msg), None, dependencyWarnings, Nil)
    }

    val recordCount = getRecordCount(source, sourceTable.query, from, to)

    minimumRecordsOpt.foreach(n => log.info(s"Minimum records to expect: $n"))

    dataChunk match {
      case Some(chunk) =>
        if (chunk.inputRecordCount == recordCount) {
          log.info(s"Table '${outputTable.name}' for $infoDate has $recordCount records (not changed). Re-sourcing is not required.")
          JobPreRunResult(JobPreRunStatus.AlreadyRan, Some(recordCount), dependencyWarnings, warnings)
        } else {
          if (recordCount >= minimumRecordsOpt.getOrElse(MINIMUM_RECORDS_DEFAULT)) {
            log.warn(s"$WARNING Table '${outputTable.name}' for $infoDate has $recordCount != ${chunk.inputRecordCount} records. The table needs re-sourced.")
            JobPreRunResult(JobPreRunStatus.NeedsUpdate, Some(recordCount), dependencyWarnings, warnings)
          } else {
            processInsufficientDataCase(infoDate, dependencyWarnings, recordCount, failIfNoData, minimumRecordsOpt, Some(chunk.inputRecordCount))
          }
        }
      case None        =>
        if (recordCount >= minimumRecordsOpt.getOrElse(MINIMUM_RECORDS_DEFAULT)) {
          log.info(s"Table '${outputTable.name}' for $infoDate has $recordCount new records. Adding to the processing list.")
          JobPreRunResult(JobPreRunStatus.Ready, Some(recordCount), dependencyWarnings, warnings)
        } else {
          processInsufficientDataCase(infoDate, dependencyWarnings, recordCount, failIfNoData, minimumRecordsOpt, None)
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

  def postProcessing(df: DataFrame,
                     infoDate: LocalDate,
                     conf: Config): DataFrame = {
    val dfTransformed = applyTransformations(df, sourceTable.transformations)

    val (from, to) = getInfoDateRange(infoDate, sourceTable.rangeFromExpr, sourceTable.rangeToExpr)

    val dfFiltered = applyFilters(dfTransformed, sourceTable.filters, infoDate, from, to)

    val dfFinal = if (sourceTable.columns.nonEmpty) {
      dfFiltered.select(sourceTable.columns.head, sourceTable.columns.tail: _*)
    } else
      dfFiltered

    dfFinal
  }

  override def save(df: DataFrame,
                    infoDate: LocalDate,
                    conf: Config,
                    jobStarted: Instant,
                    inputRecordCount: Option[Long]): SaveResult = {
    val stats = metastore.saveTable(outputTable.name, infoDate, df, inputRecordCount)

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

  private def supportsTrackDays(hasInfoDate: Boolean): Boolean = {
    if (disableCountQuery && outputTable.trackDays > 0) {
      log.info(s"The source has count query disabled for ingestion job '$name' for table '${outputTable.name}'. " +
        s"Setting track.days from ${outputTable.trackDays} to 0.")
    }
    (hasInfoDate || outputTable.trackDaysExplicitlySet) && !disableCountQuery
  }

  private def getRecordCount(source: Source, query: Query, from: LocalDate, to: LocalDate): Long = {
    if (disableCountQuery) {
      log.info(s"Getting cached record count for '${query.query}' for $from..$to...")
      getCachedDataFrame(source, query, from, to).count()
    } else {
      source.getRecordCount(sourceTable.query, from, to)
    }
  }

  private def getCachedDataFrame(source: Source, query: Query, from: LocalDate, to: LocalDate): DataFrame = {
    if (tempDirectory.isEmpty) {
      throw new IllegalArgumentException(s"When count query optimization is set on the source, a temporary directory " +
        s"in Hadoop (HDFS, S3, etc) should be set at '$TEMPORARY_DIRECTORY_KEY'.")
    }

    val cacheTableName = getVirtualTableName(query, to)
    if (TransientTableManager.hasDataForTheDate(cacheTableName, from)) {
      TransientTableManager.getDataForTheDate(cacheTableName, from)
    } else {
      val cacheDir = new Path(tempDirectory.get, "cache").toString
      val sourceDf = getData(source, query, from, to).data
      val (cachedDf, _) = TransientTableManager.addPersistedDataFrame(cacheTableName, from, sourceDf, cacheDir)
      cachedDf
    }
  }

  /**
    * Returns a table name to uniquely identify an input query for an ingestion.
    * infoDateFrom is used as a second key to transient table manager, so it it not used to form the name.
    */
  private def getVirtualTableName(query: Query, infoDateTo: LocalDate): String = {
    s"source_cache://$sourceName|${query.query}|$infoDateTo"
  }

  private def processInsufficientDataCase(infoDate: LocalDate,
                                          dependencyWarnings: Seq[DependencyWarning],
                                          recordCount: Long,
                                          failIfNoData: Boolean,
                                          minimumRecordsOpt: Option[Int],
                                          oldRecordCount: Option[Long]): JobPreRunResult = {
    minimumRecordsOpt match {
      case Some(minimumRecords) =>
        log.info(s"Table '${outputTable.name}' for $infoDate has not enough records. Minimum $minimumRecords, got $recordCount. Skipping...")
        JobPreRunResult(JobPreRunStatus.InsufficientData(recordCount, minimumRecords, oldRecordCount), minimumRecordsOpt.map(_ => recordCount), dependencyWarnings, Seq.empty[String])
      case None                 =>
        log.info(s"Table '${outputTable.name}' for $infoDate has no data (failIfNoData=$failIfNoData). Skipping...")
        JobPreRunResult(JobPreRunStatus.NoData(failIfNoData), minimumRecordsOpt.map(_ => recordCount), dependencyWarnings, Seq.empty[String])
    }
  }

  private def getSourcingResult(infoDate: LocalDate): SourceResult = {
    val (from, to) = getInfoDateRange(infoDate, sourceTable.rangeFromExpr, sourceTable.rangeToExpr)

    if (disableCountQuery) {
      log.info(s"Getting cached data for '${sourceTable.query.query}' for $from..$to...")
      SourceResult(getCachedDataFrame(source, sourceTable.query, from, to), Seq.empty, Seq.empty)
    } else {
      getData(source, sourceTable.query, from, to)
    }
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
