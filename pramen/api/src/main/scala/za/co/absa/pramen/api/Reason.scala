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

/** Reasons a Pramen job has failed. */
sealed trait Reason

object Reason {
  /** The transformation is ready to run */
  case object Ready extends Reason

  /** The transformation is ready to run, but has some warnings to be displayed in notifications. */
  case class Warning(warnings: Seq[String]) extends Reason

  /** Data required to run the job is absent or not up to date. This will result in dependent jobs not run. */
  case class NotReady(message: String) extends Reason

  /** Skip the task for the current information date, but allow dependent jobs to run. */
  case class Skip(message: String) extends Reason

  /** Skip the task only for this run, but allow dependent jobs to run. */
  case class SkipOnce(message: String) extends Reason
}
