package za.co.absa.pramen.core.mocks.databricks

import za.co.absa.pramen.core.databricks.DatabricksClient

import scala.collection.mutable.ArrayBuffer

class DatabricksClientSpy(runException: Throwable = null) extends DatabricksClient {

  val createFileInvocations = new ArrayBuffer[(String, String, Boolean)]
  val runTransientJobInvocations = new ArrayBuffer[Map[String, Any]]()

  override def createFile(content: String, destination: String, overwrite: Boolean): Unit = {
    createFileInvocations.append((content, destination, overwrite))
  }

  override def runTransientJob(jobDefinition: Map[String, Any]): Unit = {
    runTransientJobInvocations.append(jobDefinition)

    if (runException != null) {
      throw runException
    }
  }
}
