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

sealed trait RunStatus {
  val isFailure: Boolean

  def getReason: Option[String]
}

object RunStatus {
  case class Succeeded(recordCountOld: Option[Long],
                       recordCount: Long,
                       sizeBytes: Option[Long],
                       reason: TaskRunReason,
                       filesRead: Seq[String],
                       filesWritten: Seq[String],
                       hiveTablesUpdated: Seq[String],
                       warnings: Seq[String]) extends RunStatus {
    val isFailure: Boolean = false

    override def toString: String = reason.toString

    override def getReason(): Option[String] = {
      if (warnings.nonEmpty) {
        Option(warnings.mkString("\n"))
      } else {
        None
      }
    }
  }

  case class ValidationFailed(ex: Throwable) extends RunStatus {
    val isFailure: Boolean = true

    override def toString: String = "Validation failed"

    override def getReason(): Option[String] = Option(getShortExceptionDescription(ex))
  }

  case class Failed(ex: Throwable) extends RunStatus {
    val isFailure: Boolean = true

    override def toString: String = "Failed"

    override def getReason(): Option[String] = Option(getShortExceptionDescription(ex))
  }

  case class MissingDependencies(isFailure: Boolean, tables: Seq[String]) extends RunStatus {
    override def toString: String = "Dependent job failed"

    override def getReason(): Option[String] = Option(s"Dependent job failures: ${tables.mkString(", ")}")
  }

  case class FailedDependencies(isFailure: Boolean, failures: Seq[DependencyFailure]) extends RunStatus {
    override def toString: String = "Dependency check failed"

    override def getReason(): Option[String] = {
      Option(s"Dependency check failures: ${failures.map(_.renderText).mkString("; ")}")
    }
  }

  case class NoData(isFailure: Boolean) extends RunStatus {
    override def toString: String = "No data"

    override def getReason(): Option[String] = Option("No data at the source")
  }

  case class InsufficientData(actual: Long, expected: Long, recordCountOld: Option[Long]) extends RunStatus {
    override def toString: String = "Insufficient data"

    val isFailure: Boolean = true

    override def getReason(): Option[String] = Option(s"Got $actual, expected at least $expected records")
  }

  case object NotRan extends RunStatus {
    val isFailure: Boolean = false

    override def toString: String = "Not ran"

    override def getReason(): Option[String] = None
  }

  case class Skipped(msg: String, isWarning: Boolean = false) extends RunStatus {
    val isFailure: Boolean = false

    override def toString: String = "Skipped"

    override def getReason(): Option[String] = if (msg.isEmpty) None else Option(msg)
  }

  def getShortExceptionDescription(ex: Throwable): String = {
    if (ex.getCause == null) {
      ex.getMessage
    } else {
      val cause = ex.getCause
      if (cause.getCause == null) {
        s"${ex.getMessage} (${ex.getCause.getMessage})"
      } else {
        s"${ex.getMessage} (${cause.getMessage} caused by ${cause.getCause.getMessage})"
      }
    }
  }
}
