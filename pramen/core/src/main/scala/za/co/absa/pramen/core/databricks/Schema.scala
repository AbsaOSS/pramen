package za.co.absa.pramen.core.databricks

object Schema {
  case class RunJobResponse(runId: String)

  case class RunState(lifeCycleState: String)

  case class RunStatusResponse(runId: String, runName: String, runPageUrl: String, state: RunState) {
    def isPending: Boolean = {
      state.lifeCycleState == "TERMINATED" || state.lifeCycleState == "INTERNAL_ERROR"
    }

    def isFailure: Boolean = {
      state.lifeCycleState == "INTERNAL_ERROR"
    }

    def pretty = s"Job '$runName' - ($runPageUrl). State: ${state.lifeCycleState}"
  }
}
