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

import org.scalatest.wordspec.AnyWordSpec

class ResponsesSuite extends AnyWordSpec {
  "RunStatusResponse()" should {
    "know if the job is still pending" in {
      val response = getRunStatus("PENDING")

      assert(response.isJobPending)
    }

    "know if the job finished" in {
      val failedJobResponse = getRunStatus("INTERNAL_ERROR")
      val successfulJobResponse = getRunStatus("TERMINATED")

      assert(!failedJobResponse.isJobPending)
      assert(!successfulJobResponse.isJobPending)
    }

    "know if the job failed" in {
      val failedJobResponse = getRunStatus("INTERNAL_ERROR")
      val successfulJobResponse = getRunStatus("TERMINATED")
      val runningJobResponse = getRunStatus("PENDING")

      assert(failedJobResponse.isFailure)
      assert(!successfulJobResponse.isFailure)
      assert(!runningJobResponse.isFailure)
    }

    "prettify job response" in {
      val response = getRunStatus()

      assert(response.pretty == "Job 'Hello world run' - (https://example.org/run/run-id-001). State: INTERNAL_ERROR")
    }
  }

  def getRunStatus(lifeCycleState: String = "INTERNAL_ERROR") = {
    Responses.RunStatusResponse(
      "run-id-001",
      "Hello world run",
      "https://example.org/run/run-id-001",
      Responses.RunState(lifeCycleState)
    )
  }
}
