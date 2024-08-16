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

case class TaskCompletedCsv(
                             jobName: String,
                             tableName: String,
                             periodBegin: String,
                             periodEnd: String,
                             informationDate: String,
                             inputRecordCount: Long,
                             inputRecordCountOld: Long,
                             outputRecordCount: Option[Long],
                             outputRecordCountOld: Option[Long],
                             outputSize: Option[Long],
                             startedAt: Long,
                             finishedAt: Long,
                             status: String,
                             failureReason: Option[String],
                             sparkApplicationId: Option[String],
                             pipelineId: Option[String],
                             pipelineName: Option[String],
                             environmentName: Option[String],
                             tenant: Option[String]
                           )
