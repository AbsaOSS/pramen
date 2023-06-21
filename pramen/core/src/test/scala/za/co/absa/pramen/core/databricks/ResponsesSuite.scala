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
