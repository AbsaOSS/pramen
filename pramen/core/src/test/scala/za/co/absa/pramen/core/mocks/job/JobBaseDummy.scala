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

package za.co.absa.pramen.core.mocks.job

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.api.status.{DependencyWarning, TaskRunReason}
import za.co.absa.pramen.core.bookkeeper.Bookkeeper
import za.co.absa.pramen.core.metastore.Metastore
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.pipeline._
import za.co.absa.pramen.core.runner.splitter.ScheduleStrategy

import java.time.{Instant, LocalDate}

class JobBaseDummy(operationDef: OperationDef,
                   jobNotificationTargets: Seq[JobNotificationTarget],
                   metastore: Metastore,
                   bookkeeper: Bookkeeper,
                   outputTableDef: MetaTable)
  extends JobBase(operationDef, metastore, bookkeeper, jobNotificationTargets, outputTableDef) {

  override def preRunCheckJob(infoDate: LocalDate, runReason: TaskRunReason, jobConfig: Config, dependencyWarnings: Seq[DependencyWarning]): JobPreRunResult = {
    JobPreRunResult(null, None, dependencyWarnings, Seq.empty[String])
  }

  override val scheduleStrategy: ScheduleStrategy = null

  override def validate(infoDate: LocalDate, conf: Config): Reason = null

  override def run(infoDate: LocalDate, conf: Config): RunResult = null

  override def postProcessing(df: DataFrame, infoDate: LocalDate, conf: Config): DataFrame = null

  override def save(df: DataFrame, infoDate: LocalDate, conf: Config, jobStarted: Instant, inputRecordCount: Option[Long]): SaveResult = null
}
