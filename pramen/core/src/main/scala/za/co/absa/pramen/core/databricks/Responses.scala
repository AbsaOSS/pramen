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
