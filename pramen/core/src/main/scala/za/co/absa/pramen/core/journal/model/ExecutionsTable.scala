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

package za.co.absa.pramen.core.journal.model

import slick.jdbc.JdbcProfile

trait ExecutionsTable {
  val profile: JdbcProfile
  import profile.api._

  class ExecutionsRecords(tag: Tag) extends Table[Execution](tag, "executions") {
    def pipelineId = column[String]("pipelineId", O.Length(40))
    def pipelineName = column[String]("pipeline_name", O.Length(200))
    def environmentName = column[String]("environment_name", O.Length(128))
    def batchId = column[Long]("batch_id")
    def sparkApplicationId = column[String]("spark_application_id", O.Length(128))
    def computeEngineId = column[Option[String]]("compute_engine_id", O.Length(128))
    def tenant = column[Option[String]]("tenant", O.Length(200))
    def country = column[Option[String]]("country", O.Length(50))
    def runDateFrom = column[String]("run_date_from", O.Length(20))
    def runDateTo = column[Option[String]]("run_date_to", O.Length(20))
    def startedAt = column[Long]("started_at")
    def finishedAt = column[Long]("finished_at")
    def numberOfExecutorsMin = column[Option[Int]]("number_of_executors_min")
    def numberOfExecutorsMax = column[Option[Int]]("number_of_executors_max")
    def executorType = column[Option[String]]("executor_type", O.Length(128))
    def status = column[String]("status", O.Length(50))
    def isRerun = column[Boolean]("is_rerun")
    def attemptNumber = column[Int]("attempt_number")
    def numberOfAttempts = column[Int]("number_of_attempts")
    def failureReason = column[Option[String]]("failure_reason")
    def numberOfRecordsIngested = column[Option[Long]]("number_of_records_ingested")
    def maxNumberOfColumns = column[Option[Long]]("max_number_of_columns")
    def additionalOptions = column[Option[String]]("additional_options")

    private type ExecutionTuple = (
      (String, String, String, Long, String, Option[String], Option[String], Option[String], String, Option[String], Long, Long),
        (Option[Int], Option[Int], Option[String], String, Boolean, Int, Int, Option[String], Option[Long], Option[Long], Option[String])
      )

    private def toExecution(t: ExecutionTuple): Execution = Execution(
      t._1._1, t._1._2, t._1._3, t._1._4, t._1._5, t._1._6, t._1._7, t._1._8, t._1._9, t._1._10, t._1._11, t._1._12,
      t._2._1, t._2._2, t._2._3, t._2._4, t._2._5, t._2._6, t._2._7, t._2._8, t._2._9, t._2._10, t._2._11
    )

    private def fromExecution(e: Execution): Option[ExecutionTuple] = Some(
      (e.pipelineId, e.pipelineName, e.environmentName, e.batchId, e.sparkApplicationId, e.computeEngineId, e.tenant, e.country, e.runDateFrom, e.runDateTo, e.startedAt, e.finishedAt),
      (e.numberOfExecutorsMin, e.numberOfExecutorsMax, e.executorType, e.status, e.isRerun, e.attemptNumber, e.numberOfAttempts, e.failureReason, e.numberOfRecordsIngested, e.maxNumberOfColumns, e.additionalOptions)
    )

    def * = (
      (pipelineId, pipelineName, environmentName, batchId, sparkApplicationId, computeEngineId, tenant, country, runDateFrom, runDateTo, startedAt, finishedAt),
      (numberOfExecutorsMin, numberOfExecutorsMax, executorType, status, isRerun, attemptNumber, numberOfAttempts, failureReason, numberOfRecordsIngested, maxNumberOfColumns, additionalOptions)
    ) <> (toExecution, fromExecution)

    def idx1 = index("idx_exec_started_at", startedAt, unique = false)
    def idx2 = index("idx_exec_finished_at", finishedAt, unique = false)
    def idx3 = index("idx_exec_batchid", finishedAt, unique = false)
    def idx4 = index("idx_exec_compute_engine_id", finishedAt, unique = false)
    def idx5 = index("idx_exec_pipeline_id", finishedAt, unique = false)
  }

  lazy val records = TableQuery[ExecutionsRecords]
}
