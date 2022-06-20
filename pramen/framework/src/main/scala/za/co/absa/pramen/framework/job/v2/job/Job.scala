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
import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.api.metastore.{MetaTable, MetaTableStats}
import za.co.absa.pramen.framework.job.v2.OperationDef
import za.co.absa.pramen.framework.runner.splitter.ScheduleStrategy

import java.time.{Instant, LocalDate}

trait Job {
  val name: String

  val outputTable: MetaTable

  val operation: OperationDef

  val scheduleStrategy: ScheduleStrategy

  def preRunCheck(infoDate: LocalDate,
                  conf: Config): JobPreRunResult

  def validate(infoDate: LocalDate,
               conf: Config): Reason

  def run(infoDate: LocalDate,
          conf: Config): DataFrame

  def postProcessing(df: DataFrame,
                     infoDate: LocalDate,
                     conf: Config): DataFrame

  def save(df: DataFrame,
           infoDate: LocalDate,
           conf: Config,
           jobStarted: Instant,
           inputRecordCount: Option[Long]): MetaTableStats
}
