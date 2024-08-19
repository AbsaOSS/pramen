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
import za.co.absa.pramen.api.status.{DependencyWarning, TaskRunReason}
import za.co.absa.pramen.api.{Reason, Sink, Source}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.runner.splitter.ScheduleStrategy

import java.time.{Instant, LocalDate}

class TransferJob(operationDef: OperationDef,
                  metastore: Metastore,
                  bookkeeper: Bookkeeper,
                  notificationTargets: Seq[JobNotificationTarget],
                  sourceName: String,
                  source: Source,
                  table: TransferTable,
                  bookkeepingMetaTable: MetaTable,
                  sink: Sink,
                  specialCharacters: String,
                  tempDirectory: Option[String],
                  disableCountQuery: Boolean)
                 (implicit spark: SparkSession)
  extends JobBase(operationDef, metastore, bookkeeper, notificationTargets, bookkeepingMetaTable) {

  val ingestionJob = new IngestionJob(operationDef, metastore, bookkeeper, notificationTargets, sourceName, source, table.getSourceTable, bookkeepingMetaTable, specialCharacters, tempDirectory, disableCountQuery)
  val sinkJob = new SinkJob(operationDef, metastore, bookkeeper, notificationTargets, bookkeepingMetaTable, sink, table.getSinkTable)

  override val scheduleStrategy: ScheduleStrategy = ingestionJob.scheduleStrategy

  override def preRunCheckJob(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    ingestionJob.preRunCheckJob(infoDate, runReason, jobConfig, dependencyWarnings)
  }

  override def validate(infoDate: LocalDate, jobConfig: Config): Reason = {
    ingestionJob.validate(infoDate, jobConfig)
  }

  override def run(infoDate: LocalDate, conf: Config): RunResult = {
    ingestionJob.run(infoDate, conf)
  }

  def postProcessing(df: DataFrame,
                     infoDate: LocalDate,
                     conf: Config): DataFrame = {
    ingestionJob.postProcessing(df, infoDate, conf)
  }

  override def save(df: DataFrame,
                    infoDate: LocalDate,
                    conf: Config,
                    jobStarted: Instant,
                    inputRecordCount: Option[Long]): SaveResult = {
    sinkJob.save(df, infoDate, conf, jobStarted, inputRecordCount)
  }
}
