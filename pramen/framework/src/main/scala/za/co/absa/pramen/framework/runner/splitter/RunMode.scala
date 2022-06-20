package za.co.absa.pramen.framework.runner.splitter

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
