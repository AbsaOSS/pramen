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

sealed trait QuotingPolicy {
  def name: String
}

object QuotingPolicy {
  case object Never extends QuotingPolicy {
    override def name: String = "never"
  }

  case object Always extends QuotingPolicy {
    override def name: String = "always"
  }

  case object Auto extends QuotingPolicy {
    override def name: String = "auto"
  }

  def fromString(s: String): QuotingPolicy = {
    s.toLowerCase match {
      case "never" => Never
      case "always" => Always
      case "auto" => Auto
      case _ => throw new IllegalArgumentException(s"Unknown quoting policy: '$s'. Should be one of 'auto' (default), 'never', 'always'.")
    }
  }
}
