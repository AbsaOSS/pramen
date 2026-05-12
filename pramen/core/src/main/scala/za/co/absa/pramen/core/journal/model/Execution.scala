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

case class Execution(
                      pipelineId: String,
                      pipelineName: String,
                      environmentName: String,
                      batchId: Long,
                      sparkApplicationId: String,
                      computeEngineId: Option[String],
                      tenant: Option[String],
                      country: Option[String],
                      runDateFrom: String,
                      runDateTo: Option[String],
                      startedAt: Long,
                      finishedAt: Long,
                      numberOfExecutorsMin: Option[Int],
                      numberOfExecutorsMax: Option[Int],
                      executorType: Option[String],
                      status: String,
                      isRerun: Boolean,
                      attemptNumber: Int,
                      numberOfAttempts: Int,
                      failureReason: Option[String],
                      numberOfRecordsIngested: Option[Long],
                      maxNumberOfColumns: Option[Long],
                      additionalOptions: Option[String]
                    )
