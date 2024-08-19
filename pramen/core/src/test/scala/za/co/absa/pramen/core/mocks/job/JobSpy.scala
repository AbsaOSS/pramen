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
import org.apache.spark.sql.types.StructType
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.api.{DataFormat, Reason}
import za.co.absa.pramen.core.OperationDefFactory
import za.co.absa.pramen.core.metastore.MetaTableStats
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.mocks.MetaTableFactory.getDummyMetaTable
import za.co.absa.pramen.core.pipeline._
import za.co.absa.pramen.core.runner.splitter.{ScheduleStrategy, ScheduleStrategySourcing}

import java.time.{Instant, LocalDate}

class JobSpy(jobName: String = "DummyJob",
             outputTableIn: String = "table_out",
             outputTableFormat: DataFormat = DataFormat.Parquet("/tmp/dummy", None),
             hiveTable: Option[String] = None,
             operationDef: OperationDef = OperationDefFactory.getDummyOperationDef(),
             preRunCheckFunction: () => JobPreRunResult = () => JobPreRunResult(JobPreRunStatus.Ready, None, Nil, Nil),
             validationFunction: () => Reason = () => Reason.Ready,
             runFunction: () => RunResult = () => null,
             scheduleStrategyIn: ScheduleStrategy = new ScheduleStrategySourcing,
             allowParallel: Boolean = true,
             saveStats: MetaTableStats = MetaTableStats(0, None),
             jobNotificationTargets: Seq[JobNotificationTarget] = Seq.empty,
             jobTrackDays: Int = 0
            ) extends Job {
  var getDatesToRunCount = 0
  var preRunCheckCount = 0
  var validateCount = 0
  var runCount = 0
  var postProcessingCount = 0
  var saveCount = 0
  var saveDf: DataFrame = _
  var createHiveTableCount = 0

  override val name: String = jobName

  override val outputTable: MetaTable = getDummyMetaTable(outputTableIn, format = outputTableFormat, hiveTable = hiveTable)

  override val operation: OperationDef = operationDef

  override val scheduleStrategy: ScheduleStrategy = scheduleStrategyIn

  override def allowRunningTasksInParallel: Boolean = allowParallel

  override def notificationTargets: Seq[JobNotificationTarget] = jobNotificationTargets

  override def trackDays: Int = jobTrackDays

  def preRunCheck(infoDate: LocalDate,
                  runReason: TaskRunReason,
                  conf: Config): JobPreRunResult = {
    preRunCheckCount += 1
    preRunCheckFunction()
  }

  override def validate(infoDate: LocalDate, conf: Config): Reason = {
    validateCount += 1
    validationFunction()
  }

  override def run(infoDate: LocalDate, conf: Config): RunResult = {
    runCount += 1
    runFunction()
  }

  override def postProcessing(df: DataFrame, infoDate: LocalDate, conf: Config): DataFrame = {
    postProcessingCount += 1
    df
  }

  override def save(df: DataFrame, infoDate: LocalDate, jobConfig: Config, jobStarted: Instant, inputRecordCount: Option[Long]): SaveResult = {
    saveDf = df
    saveCount += 1

    SaveResult(saveStats)
  }

  override def createOrRefreshHiveTable(schema: StructType, infoDate: LocalDate, recreate: Boolean): Seq[String] = {
    createHiveTableCount += 1
    Nil
  }
}
