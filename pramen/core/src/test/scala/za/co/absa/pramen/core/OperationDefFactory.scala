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

package za.co.absa.pramen.core

import com.typesafe.config.{Config, ConfigFactory}
import za.co.absa.pramen.api.status.MetastoreDependency
import za.co.absa.pramen.core.pipeline.{OperationDef, OperationType, TransformExpression}
import za.co.absa.pramen.core.schedule.Schedule

object OperationDefFactory {

  def getDummyOperationDef(name: String = "DummyOperation",
                           operationConf: Config = ConfigFactory.empty(),
                           operationType: OperationType = OperationType.Transformation("dummy.class", "dummy_output_table"),
                           schedule: Schedule = Schedule.EveryDay(),
                           expectedDelayDays: Int = 0,
                           allowParallel: Boolean = true,
                           alwaysAttempt: Boolean = false,
                           consumeThreads: Int = 1,
                           dependencies: Seq[MetastoreDependency] = Nil,
                           outputInfoDateExpression: String = "@date",
                           initialSourcingDateExpression: String = "@runDate",
                           processingTimestampColumn: Option[String] = None,
                           warnMaxExecutionTimeSeconds: Option[Int] = None,
                           killMaxExecutionTimeSeconds: Option[Int] = None,
                           schemaTransformations: Seq[TransformExpression] = Nil,
                           filters: Seq[String] = Nil,
                           notificationTargets: Seq[String] = Nil,
                           sparkConfig: Map[String, String] = Map.empty[String, String],
                           extraOptions: Map[String, String] = Map.empty[String, String]): OperationDef = {
    OperationDef(
      name,
      operationConf,
      operationType,
      schedule,
      expectedDelayDays,
      allowParallel,
      alwaysAttempt,
      consumeThreads,
      dependencies,
      outputInfoDateExpression,
      initialSourcingDateExpression,
      processingTimestampColumn,
      warnMaxExecutionTimeSeconds,
      killMaxExecutionTimeSeconds,
      schemaTransformations,
      filters,
      notificationTargets,
      sparkConfig,
      extraOptions)
  }
}
