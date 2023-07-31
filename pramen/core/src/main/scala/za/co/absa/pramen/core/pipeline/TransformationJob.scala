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
import za.co.absa.pramen.api.{Reason, Transformer}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.runner.splitter.{ScheduleStrategy, ScheduleStrategySourcing}

import java.time.{Instant, LocalDate}

class TransformationJob(operationDef: OperationDef,
                        metastore: Metastore,
                        bookkeeper: Bookkeeper,
                        notificationTargets: Seq[JobNotificationTarget],
                        outputTable: MetaTable,
                        transformer: Transformer)
                       (implicit spark: SparkSession)
  extends JobBase(operationDef, metastore, bookkeeper, notificationTargets, outputTable) {

  private val inputTables = operationDef.dependencies.flatMap(_.tables).distinct

  override val scheduleStrategy: ScheduleStrategy = new ScheduleStrategySourcing

  override def preRunCheckJob(infoDate: LocalDate, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    preRunTransformationCheck(infoDate, dependencyWarnings)
  }

  override def validate(infoDate: LocalDate, jobConfig: Config): Reason = {
    transformer.validate(metastore.getMetastoreReader(inputTables, infoDate), infoDate, effectiveExtraOptions)
  }

  override def run(infoDate: LocalDate, conf: Config): RunResult = {
    RunResult(transformer.run(metastore.getMetastoreReader(inputTables, infoDate), infoDate, effectiveExtraOptions))
  }

  def postProcessing(df: DataFrame,
                     infoDate: LocalDate,
                     conf: Config): DataFrame = {
    df
  }

  override def save(df: DataFrame,
                    infoDate: LocalDate,
                    conf: Config,
                    jobStarted: Instant,
                    inputRecordCount: Option[Long]): SaveResult = {
    SaveResult(metastore.saveTable(outputTable.name, infoDate, df, None))
  }
}
