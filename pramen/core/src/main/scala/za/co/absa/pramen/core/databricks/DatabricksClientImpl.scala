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

import org.slf4j.LoggerFactory
import requests.RequestAuth
import za.co.absa.pramen.core.databricks.Schema.{RunJobResponse, RunStatusResponse}
import za.co.absa.pramen.core.utils.JsonUtils

import java.util.Base64

class DatabricksClientImpl(host: String, token: String) extends DatabricksClient {
  import DatabricksClientImpl._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val auth = RequestAuth.Bearer(token)

  override def createFile(content: String, destination: String, overwrite: Boolean = false): Unit = {
    val url = getCreateFileUrl(host)
    val encodedContent = encodeString(content)

    val payload = requests.MultiPart(
      requests.MultiItem("path", destination),
      requests.MultiItem("contents", encodedContent),
      requests.MultiItem("overwrite", overwrite.toString)
    )

    requests.post(url, data = payload, auth = auth)
  }

  override def runTransientJob(jobDefinition: Map[String, Any]): Unit = {
    val url = getRunSubmitUrl(host)
    val serializedJobDefinition = JsonUtils.asJson(jobDefinition)

    val response = requests.post(url, auth = auth, data = serializedJobDefinition)

    if (response.is2xx) {
      val responseContent = response.data.toString()
      val runStatus = JsonUtils.fromJson[RunJobResponse](responseContent)

      waitForSuccessfulFinish(runStatus.runId)
    } else {
      throw new RuntimeException(s"Could not submit a run to Databricks. Response: ${response.data.toString()}")
    }
  }

  private def waitForSuccessfulFinish(runId: String): Unit = {
    var runStatus = getRunStatus(runId)

    while (runStatus.isPending) {
      log.info(s"Waiting for: ${runStatus.pretty}.")
      Thread.sleep(5000)

      runStatus = getRunStatus(runId)
    }

    if (runStatus.isFailure) {
      throw new RuntimeException(s"Failed job: ${runStatus.pretty}.")
    }
  }

  private def getRunStatus(runId: String): RunStatusResponse = {
    val url = getRunStatusUrl(host, runId)
    val response = requests.get(url, auth = auth)

    val responseContent = response.data.toString()
    JsonUtils.fromJson[RunStatusResponse](responseContent)
  }
}

object DatabricksClientImpl {
  private def getCreateFileUrl(host: String): String = {
    s"$host/api/2.0/dbfs/put"
  }

  private def getRunSubmitUrl(host: String): String = {
    s"$host/api/2.1/jobs/runs/submit"
  }

  private def getRunStatusUrl(host: String, runId: String): String = {
    s"$host/api/2.1/jobs/runs/get?run_id=$runId"
  }

  private def encodeString(string: String) = {
    Base64.getEncoder.encodeToString(string.getBytes)
  }
}
