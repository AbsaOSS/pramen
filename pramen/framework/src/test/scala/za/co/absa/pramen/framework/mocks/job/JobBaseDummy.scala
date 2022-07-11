/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.pramen.framework.mocks.job

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.framework.bookkeeper.Bookkeeper
import za.co.absa.pramen.framework.metastore.model.MetaTable
import za.co.absa.pramen.framework.metastore.{MetaTableStats, Metastore}
import za.co.absa.pramen.framework.pipeline.{DependencyWarning, JobBase, JobPreRunResult, OperationDef}
import za.co.absa.pramen.framework.runner.splitter.ScheduleStrategy

import java.time.{Instant, LocalDate}

class JobBaseDummy(operationDef: OperationDef,
                   metastore: Metastore,
                   bookkeeper: Bookkeeper,
                   outputTableDef: MetaTable)
  extends JobBase(operationDef, metastore, bookkeeper, outputTableDef) {

  override def preRunCheckJob(infoDate: LocalDate, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    JobPreRunResult(null, None, dependencyWarnings)
  }

  override val scheduleStrategy: ScheduleStrategy = null

  override def validate(infoDate: LocalDate, conf: Config): Reason = null

  override def run(infoDate: LocalDate, conf: Config): DataFrame = null

  override def postProcessing(df: DataFrame, infoDate: LocalDate, conf: Config): DataFrame = null

  override def save(df: DataFrame, infoDate: LocalDate, conf: Config, jobStarted: Instant, inputRecordCount: Option[Long]): MetaTableStats = null
}
