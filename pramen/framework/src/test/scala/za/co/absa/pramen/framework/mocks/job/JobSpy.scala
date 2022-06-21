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
import za.co.absa.pramen.api.metastore.{MetaTable, MetaTableStats}
import za.co.absa.pramen.framework.OperationDefFactory
import za.co.absa.pramen.framework.pipeline.{Job, JobPreRunResult, JobPreRunStatus, OperationDef}
import za.co.absa.pramen.framework.mocks.MetaTableFactory.getDummyMetaTable
import za.co.absa.pramen.framework.runner.splitter.{ScheduleStrategy, ScheduleStrategySourcing}

import java.time.{Instant, LocalDate}

class JobSpy(jobName: String = "DummyJob",
             outputTableIn: String = "table_out",
             operationDef: OperationDef = OperationDefFactory.getDummyOperationDef(),
             preRunCheckFunction: () => JobPreRunResult = () => JobPreRunResult(JobPreRunStatus.Ready, None, Nil),
             validationFunction: () => Reason = () => Reason.Ready,
             runFunction: () => DataFrame = () => null,
             scheduleStrategyIn: ScheduleStrategy = new ScheduleStrategySourcing,
             saveStats: MetaTableStats = MetaTableStats(0, None)
             ) extends Job {
  var getDatesToRunCount = 0
  var preRunCheckCount = 0
  var validateCount = 0
  var runCount = 0
  var postProcessingCount = 0
  var saveCount = 0
  var saveDf: DataFrame = _

  override val name: String = jobName

  override val outputTable: MetaTable = getDummyMetaTable(outputTableIn)

  override val operation: OperationDef = operationDef

  override val scheduleStrategy: ScheduleStrategy = scheduleStrategyIn

  def preRunCheck(infoDate: LocalDate,
                  conf: Config): JobPreRunResult = {
    preRunCheckCount += 1
    preRunCheckFunction()
  }

  override def validate(infoDate: LocalDate, conf: Config): Reason = {
    validateCount += 1
    validationFunction()
  }

  override def run(infoDate: LocalDate, conf: Config): DataFrame = {
    runCount += 1
    runFunction()
  }

  override def postProcessing(df: DataFrame, infoDate: LocalDate, conf: Config): DataFrame = {
    postProcessingCount += 1
    df
  }

  override def save(df: DataFrame, infoDate: LocalDate, jobConfig: Config, jobStarted: Instant, inputRecordCount: Option[Long]): MetaTableStats = {
    saveDf  = df
    saveCount += 1

    saveStats
  }

}
