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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{Reason, Source, SourceResult}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.metastore.{MetaTableStats, Metastore}
import za.co.absa.pramen.core.runner.splitter.{ScheduleStrategy, ScheduleStrategySourcing}
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.Emoji.WARNING
import za.co.absa.pramen.core.utils.SparkUtils._

import java.time.{Instant, LocalDate}
import scala.collection.mutable.ListBuffer

class IngestionJob(operationDef: OperationDef,
                   metastore: Metastore,
                   bookkeeper: Bookkeeper,
                   notificationTargets: Seq[JobNotificationTarget],
                   source: Source,
                   sourceTable: SourceTable,
                   outputTable: MetaTable,
                   specialCharacters: String)
                  (implicit spark: SparkSession)
  extends JobBase(operationDef, metastore, bookkeeper, notificationTargets, outputTable) {
  import JobBase._

  private val log = LoggerFactory.getLogger(this.getClass)

  override val scheduleStrategy: ScheduleStrategy = new ScheduleStrategySourcing

  override def preRunCheckJob(infoDate: LocalDate, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    source.connect()

    val dataChunk = bookkeeper.getLatestDataChunk(sourceTable.metaTableName, infoDate, infoDate)

    val (from, to) = getInfoDateRange(infoDate, sourceTable.rangeFromExpr, sourceTable.rangeToExpr)

    val recordCount = source.getRecordCount(sourceTable.query, from, to)

    val minimumRecordsOpt = ConfigUtils.getOptionInt(source.config, MINIMUM_RECORDS_KEY)
    val failIfNoData = ConfigUtils.getOptionBoolean(source.config, FAIL_NO_DATA_KEY).getOrElse(false)

    minimumRecordsOpt.foreach(n => log.info(s"Minimum records to expect: $n"))

    dataChunk match {
      case Some(chunk) =>
        if (chunk.inputRecordCount == recordCount) {
          log.info(s"Table '${outputTable.name}' for $infoDate has $recordCount records (not changed). Re-sourcing is not required.")
          JobPreRunResult(JobPreRunStatus.AlreadyRan, Some(recordCount), dependencyWarnings)
        } else {
          if (recordCount >= minimumRecordsOpt.getOrElse(MINIMUM_RECORDS_DEFAULT)) {
            log.warn(s"$WARNING Table '${outputTable.name}' for $infoDate has $recordCount != ${chunk.inputRecordCount} records. The table needs re-sourced.")
            JobPreRunResult(JobPreRunStatus.NeedsUpdate, Some(recordCount), dependencyWarnings)
          } else {
            processInsufficientDataCase(infoDate, dependencyWarnings, recordCount, failIfNoData, minimumRecordsOpt, Some(chunk.inputRecordCount))
          }
        }
      case None        =>
        if (recordCount >= minimumRecordsOpt.getOrElse(MINIMUM_RECORDS_DEFAULT)) {
          log.info(s"Table '${outputTable.name}' for $infoDate has $recordCount new records. Adding to the processing list.")
          JobPreRunResult(JobPreRunStatus.Ready, Some(recordCount), dependencyWarnings)
        } else {
          processInsufficientDataCase(infoDate, dependencyWarnings, recordCount, failIfNoData, minimumRecordsOpt, None)
        }
    }
  }

  private def processInsufficientDataCase(infoDate: LocalDate,
                                          dependencyWarnings: Seq[DependencyWarning],
                                          recordCount: Long,
                                          failIfNoData: Boolean,
                                          minimumRecordsOpt: Option[Int],
                                          oldRecordCount: Option[Long]) = {
    minimumRecordsOpt match {
      case Some(minimumRecords) =>
        log.info(s"Table '${outputTable.name}' for $infoDate has not enough records. Minimum $minimumRecords, got $recordCount. Skipping...")
        JobPreRunResult(JobPreRunStatus.InsufficientData(recordCount, minimumRecords, oldRecordCount), minimumRecordsOpt.map(_ => recordCount), dependencyWarnings)
      case None                 =>
        log.info(s"Table '${outputTable.name}' for $infoDate has no data. Skipping...")
        JobPreRunResult(JobPreRunStatus.NoData(failIfNoData), minimumRecordsOpt.map(_ => recordCount), dependencyWarnings)
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
    source.close()

    SaveResult(stats)
  }

  private def getSourcingResult(infoDate: LocalDate): SourceResult = {
    val (from, to) = getInfoDateRange(infoDate, sourceTable.rangeFromExpr, sourceTable.rangeToExpr)

    val sourceResult = source.getData(sourceTable.query, from, to, sourceTable.columns)

    val sanitizedDf = sanitizeDfColumns(sourceResult.data, specialCharacters)

    sourceResult.copy(data = sanitizedDf)
  }
}
