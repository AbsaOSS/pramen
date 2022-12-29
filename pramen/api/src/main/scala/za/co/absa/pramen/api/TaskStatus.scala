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

package za.co.absa.pramen.api

sealed trait TaskStatus

object TaskStatus {
  case class Succeeded(recordCount: Long) extends TaskStatus

  case class ValidationFailed(ex: Throwable) extends TaskStatus

  case class Failed(ex: Throwable) extends TaskStatus

  case class MissingDependencies(tables: Seq[String]) extends TaskStatus

  case class FailedDependencies(tables: Seq[String]) extends TaskStatus

  case object NoData extends TaskStatus

  case class InsufficientData(actual: Long, expected: Long) extends TaskStatus

  case class Skipped(msg: String) extends TaskStatus
}

