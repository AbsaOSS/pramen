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

sealed trait TaskStatus

object TaskStatus {
  case object NEW extends TaskStatus {
    override val toString: String = "New"
  }

  case object UPDATE extends TaskStatus {
    override val toString: String = "Update"
  }

  case object RENEW extends TaskStatus {
    override val toString: String = "Renew"
  }

  case object RERUN extends TaskStatus {
    override val toString: String = "Rerun"
  }

  case object LATE extends TaskStatus {
    override val toString: String = "Late"
  }

  case object SKIPPED extends TaskStatus {
    override val toString: String = "Skipped"
  }

  case object NOT_READY extends TaskStatus {
    override val toString: String = "Not ready"
  }

  case object FAILED extends TaskStatus {
    override val toString: String = "Failed"
  }

  def fromString(s: String): Option[TaskStatus] = {
    s match {
      case NEW.toString => Some(NEW)
      case UPDATE.toString => Some(UPDATE)
      case RENEW.toString => Some(RENEW)
      case RERUN.toString => Some(RERUN)
      case LATE.toString => Some(LATE)
      case SKIPPED.toString => Some(SKIPPED)
      case NOT_READY.toString => Some(NOT_READY)
      case FAILED.toString => Some(FAILED)
      case _ => None
    }
  }

  def isFailure(statusString: String): Boolean = {
    fromString(statusString).exists(t => t == FAILED || t == NOT_READY)
  }

}
