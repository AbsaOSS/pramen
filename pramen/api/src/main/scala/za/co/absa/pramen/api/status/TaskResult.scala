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

package za.co.absa.pramen.api.status

import za.co.absa.pramen.api.{MetaTableDef, SchemaDifference}

case class TaskResult(
                       jobName: String,
                       outputTable: MetaTableDef,
                       runStatus: RunStatus,
                       runInfo: Option[RunInfo],
                       applicationId: String,
                       isTransient: Boolean,
                       isRawFilesJob: Boolean,
                       schemaChanges: Seq[SchemaDifference],
                       dependencyWarnings: Seq[DependencyWarning],
                       notificationTargetErrors: Seq[NotificationFailure],
                       options: Map[String, String]
                     )
