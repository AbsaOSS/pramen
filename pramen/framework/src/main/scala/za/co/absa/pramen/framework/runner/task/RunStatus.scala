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

package za.co.absa.pramen.framework.runner.task

import za.co.absa.pramen.framework.job.v2.job.{DependencyFailure, TaskRunReason}

sealed trait RunStatus {
  val isFailure: Boolean
}

object RunStatus {
  case class Succeeded(recordCountOld: Option[Long], recordCount: Long, sizeBytes: Option[Long], reason: TaskRunReason) extends RunStatus {
    val isFailure: Boolean = false
  }

  case class ValidationFailed(ex: Throwable) extends RunStatus {
    val isFailure: Boolean = true
  }

  case class Failed(ex: Throwable) extends RunStatus {
    val isFailure: Boolean = true
  }

  case class MissingDependencies(tables: Seq[String]) extends RunStatus {
    val isFailure: Boolean = true
  }

  case class FailedDependencies(failures: Seq[DependencyFailure]) extends RunStatus {
    val isFailure: Boolean = true
  }

  case object NoData extends RunStatus {
    val isFailure: Boolean = false
  }

  case object NotRan extends RunStatus {
    val isFailure: Boolean = false
  }

  case class Skipped(msg: String) extends RunStatus {
    val isFailure: Boolean = false
  }
}
