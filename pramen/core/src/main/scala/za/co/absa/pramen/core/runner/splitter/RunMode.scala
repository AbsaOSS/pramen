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

package za.co.absa.pramen.core.runner.splitter

trait RunMode

object RunMode {
  /** If data is available for the information day - skip, otherwise run (fill gaps). */
  case object SkipAlreadyRan extends RunMode {
    override val toString = "fill_gaps"
  }

  /** Fills gaps as above, but also checks if retrospective updates are needed (e.g. source data has changed). */
  case object CheckUpdates extends RunMode {
    override val toString = "check_updates"
  }

  /** Reruns all eligible information dates for the specified period, even if already ran. */
  case object ForceRun extends RunMode {
    override val toString = "force"
  }

  def fromString(s: String): RunMode = s match {
    case "fill_gaps" => SkipAlreadyRan
    case "check_updates" => CheckUpdates
    case "force" => ForceRun
    case _ => throw new IllegalArgumentException(s"Unknown historical run mode: $s (should be one of: 'fill_gaps', 'check_updates', 'force')")
  }
}
