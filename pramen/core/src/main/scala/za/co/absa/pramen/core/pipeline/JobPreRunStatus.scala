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

import za.co.absa.pramen.api.status.DependencyFailure

trait JobPreRunStatus

object JobPreRunStatus {
  case object Ready extends JobPreRunStatus
  case object AlreadyRan extends JobPreRunStatus
  case object NeedsUpdate extends JobPreRunStatus
  case class Skip(msg: String) extends JobPreRunStatus
  case class NoData(isFailure: Boolean) extends JobPreRunStatus
  case class InsufficientData(actual: Long, expected: Long, oldRecordCount: Option[Long]) extends JobPreRunStatus
  case class FailedDependencies(isFailure: Boolean, failures: Seq[DependencyFailure]) extends JobPreRunStatus
}
