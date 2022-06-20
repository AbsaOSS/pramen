/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.job.v2.job

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.api.metastore.{MetaTable, MetaTableStats, Metastore}
import za.co.absa.pramen.api.v2.Source
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.job.SourceTable
import za.co.absa.pramen.framework.job.v2.OperationDef
import za.co.absa.pramen.framework.runner.splitter.{ScheduleStrategy, ScheduleStrategySourcing}
import za.co.absa.pramen.framework.utils.SparkUtils._

import java.time.{Instant, LocalDate}

class IngestionJob(operationDef: OperationDef,
                   metastore: Metastore,
                   bookkeeper: SyncBookKeeper,
                   source: Source,
                   sourceTable: SourceTable,
                   outputTable: MetaTable)
                  (implicit spark: SparkSession)
  extends JobBase(operationDef, metastore, bookkeeper, outputTable) {

  private val log = LoggerFactory.getLogger(this.getClass)

  override val scheduleStrategy: ScheduleStrategy = new ScheduleStrategySourcing

  override def preRunCheckJob(infoDate: LocalDate, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    val dataChunk = bookkeeper.getLatestDataChunk(sourceTable.metaTableName, infoDate, infoDate)

    val reader = source.getReader(sourceTable.query, sourceTable.columns)
    val recordCount = reader.getRecordCount(infoDate, infoDate)

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
    val (from, to) = getInfoDateRange(infoDate, sourceTable.sinkFromExpr, sourceTable.sinkToExpr)

    source.getReader(sourceTable.query, Nil).getData(from, to) match {
      case Some(df) => df
      case None     => throw new RuntimeException(s"Failed to read data from '${sourceTable.query.query}'")
    }
  }
}
