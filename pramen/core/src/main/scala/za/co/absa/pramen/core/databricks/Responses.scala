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

package za.co.absa.pramen.core.databricks

object Responses {
  case class RunJobResponse(run_id: Long)

  case class RunState(life_cycle_state: String)

  case class RunStatusResponse(run_id: String, run_name: String, run_page_url: String, state: RunState) {
    def isJobPending: Boolean = {
      state.life_cycle_state != "TERMINATED" && state.life_cycle_state != "INTERNAL_ERROR"
    }

    def isFailure: Boolean = {
      state.life_cycle_state == "INTERNAL_ERROR"
    }

    def pretty = s"Job '$run_name' - ($run_page_url). State: ${state.life_cycle_state}"
  }
}
