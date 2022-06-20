package za.co.absa.pramen.framework.runner

import za.co.absa.pramen.api.Job

trait JobRunner {
  def runJob(job: Job): Unit
}
