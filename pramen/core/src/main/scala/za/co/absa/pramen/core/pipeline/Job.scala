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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import za.co.absa.pramen.api.status.TaskRunReason
import za.co.absa.pramen.api.{Query, Reason}
import za.co.absa.pramen.core.metastore.model.MetaTable
import za.co.absa.pramen.core.runner.splitter.ScheduleStrategy

import java.time.{Instant, LocalDate}

trait Job {
  val name: String

  val outputTable: MetaTable

  val operation: OperationDef

  val scheduleStrategy: ScheduleStrategy

  def allowRunningTasksInParallel: Boolean

  def notificationTargets: Seq[JobNotificationTarget]

  def trackDays: Int

  /**
    * Checks pre-conditions for the job, such as data availability.
    */
  def preRunCheck(infoDate: LocalDate,
                  runReason: TaskRunReason,
                  conf: Config): JobPreRunResult

  /**
    * Validates if the job can run. This is the business side of the validation.
    */
  def validate(infoDate: LocalDate,
               conf: Config): Reason

  /**
    * Runs the business logic of the job, resulting in a DataFrame.
    */
  def run(infoDate: LocalDate,
          conf: Config): RunResult

  /**
    * Apply all postprocessing (transformations, filters, etc) steps defined in the configuration to the dataframe.
    */
  def postProcessing(df: DataFrame,
                     infoDate: LocalDate,
                     conf: Config): DataFrame

  /**
    * Saves the results. The semantics depend on the job type.
    * - For ingestion and transformation jobs, the results are saved to the metastore.
    * - For sink jobs the results are send to the sink.
    *
    * @param df               The dataframe with all transformations and projections applied.
    * @param infoDate         The information date for the partitioning.
    * @param conf             The job configuration.
    * @param jobStarted       The time when the job started.
    * @param inputRecordCount The number of records in the input dataframe if is was already determined.
    * @return The list of warnings if Hive errors are ignored.
    */
  def save(df: DataFrame,
           infoDate: LocalDate,
           conf: Config,
           jobStarted: Instant,
           inputRecordCount: Option[Long]): SaveResult

  /**
    * Creates or repairs a Hive table for the given metastore table.
    *
    * @param schema   The schema of the Hive table
    * @param infoDate The information date for which to update the Hive table
    * @param recreate Whether to force recreate the Hive table
    * @return The list of warnings if Hive errors are ignored.
    */
  def createOrRefreshHiveTable(schema: StructType, infoDate: LocalDate, recreate: Boolean): Seq[String]
}
