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
import za.co.absa.pramen.api.{Reason, Source}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.metastore.{MetaTableStats, Metastore}
import za.co.absa.pramen.core.runner.splitter.{ScheduleStrategy, ScheduleStrategySourcing}
import za.co.absa.pramen.core.utils.SparkUtils._

import java.time.{Instant, LocalDate}

class IngestionJob(operationDef: OperationDef,
                   metastore: Metastore,
                   bookkeeper: Bookkeeper,
                   source: Source,
                   sourceTable: SourceTable,
                   outputTable: MetaTable)
                  (implicit spark: SparkSession)
  extends JobBase(operationDef, metastore, bookkeeper, outputTable) {

  private val log = LoggerFactory.getLogger(this.getClass)

  override val scheduleStrategy: ScheduleStrategy = new ScheduleStrategySourcing

  override def preRunCheckJob(infoDate: LocalDate, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    val dataChunk = bookkeeper.getLatestDataChunk(sourceTable.metaTableName, infoDate, infoDate)

    val (from, to) = getInfoDateRange(infoDate, sourceTable.rangeFromExpr, sourceTable.rangeToExpr)

    val reader = source.getReader(sourceTable.query, sourceTable.columns)
    val recordCount = reader.getRecordCount(from, to)

    dataChunk match {
      case Some(chunk) =>
        if (chunk.inputRecordCount == recordCount) {
          log.info(s"Table '${outputTable.name}' for $infoDate has $recordCount records (not changed). Souring is not needed.")
          JobPreRunResult(JobPreRunStatus.AlreadyRan, Some(recordCount), dependencyWarnings)
        } else {
          log.info(s"Table '${outputTable.name}' for $infoDate has $recordCount != ${chunk.outputRecordCount} records. The table needs update.")
          JobPreRunResult(JobPreRunStatus.NeedsUpdate, Some(recordCount), dependencyWarnings)
        }
      case None        =>
        if (recordCount > 0) {
          log.info(s"Table '${outputTable.name}' for $infoDate has $recordCount new records. Adding to the processing list.")
          JobPreRunResult(JobPreRunStatus.Ready, Some(recordCount), dependencyWarnings)
        } else {
          log.info(s"Table '${outputTable.name}' for $infoDate has no data. Skipping...")
          JobPreRunResult(JobPreRunStatus.NoData, None, dependencyWarnings)
        }
    }
  }

  override def validate(infoDate: LocalDate, jobConfig: Config): Reason = {
    Reason.Ready
  }

  override def run(infoDate: LocalDate, conf: Config): DataFrame = {
    getSourcingDataFrame(infoDate)
  }

  def postProcessing(df: DataFrame,
                     infoDate: LocalDate,
                     conf: Config): DataFrame = {
    val dfTransformed = applyTransformations(df, sourceTable.transformations)

    val dfFiltered = applyFilters(dfTransformed, sourceTable.filters, infoDate)

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
                    inputRecordCount: Option[Long]): MetaTableStats = {
    metastore.saveTable(outputTable.name, infoDate, df, inputRecordCount)
  }

  private def getSourcingDataFrame(infoDate: LocalDate): DataFrame = {
    val (from, to) = getInfoDateRange(infoDate, sourceTable.rangeFromExpr, sourceTable.rangeToExpr)

    source.getReader(sourceTable.query, Nil).getData(from, to) match {
      case Some(df) => sanitizeDfColumns(df)
      case None     => throw new RuntimeException(s"Failed to read data from '${sourceTable.query.query}'")
    }
  }
}
