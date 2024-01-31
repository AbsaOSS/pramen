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

package za.co.absa.pramen.api.sql

sealed trait SqlColumnType

object SqlColumnType {
  case object DATE extends SqlColumnType {
    override val toString: String = "date"
  }

  case object STRING extends SqlColumnType {
    override val toString: String = "string"
  }

  case object NUMBER extends SqlColumnType {
    override val toString: String = "number"
  }

  case object DATETIME extends SqlColumnType {
    override val toString: String = "datetime"
  }

  def fromString(s: String): Option[SqlColumnType] = {
    s.toLowerCase() match {
      case DATE.toString     => Some(DATE)
      case DATETIME.toString => Some(DATETIME)
      case STRING.toString   => Some(STRING)
      case NUMBER.toString   => Some(NUMBER)
      case _ => None
    }
  }
}
