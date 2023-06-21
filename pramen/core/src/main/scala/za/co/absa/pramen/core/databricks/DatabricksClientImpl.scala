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
import za.co.absa.pramen.core.databricks.Responses.{RunJobResponse, RunStatusResponse}
import za.co.absa.pramen.core.utils.{JsonUtils, StringUtils}

class DatabricksClientImpl(host: String, token: String) extends DatabricksClient {
  import DatabricksClientImpl._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val auth = RequestAuth.Bearer(token)

  private val baseUrl = host.stripSuffix("/")

  override def createFile(content: String, destination: String, overwrite: Boolean = false): Unit = {
    val url = getCreateFileUrl(baseUrl)
    val encodedContent = StringUtils.encodeToBase64(content)

    val payload = requests.MultiPart(
      requests.MultiItem("path", destination),
      requests.MultiItem("contents", encodedContent),
      requests.MultiItem("overwrite", overwrite.toString)
    )

    val response = requests.post(url, data = payload, auth = auth)

    if (response.is4xx) {
      throw new RuntimeException(s"Failed to create file at $destination: ${response.data}")
    }
  }

  override def runTransientJob(jobDefinition: Map[String, Any]): Unit = {
    val url = getRunSubmitUrl(baseUrl)
    val serializedJobDefinition = JsonUtils.asJson(jobDefinition)

    val response = requests.post(url, auth = auth, data = serializedJobDefinition)

    if (response.is2xx) {
      val responseContent = response.data.toString()
      val runStatus = JsonUtils.fromJson[RunJobResponse](responseContent)

      waitForSuccessfulFinish(runStatus.run_id)
    } else {
      throw new RuntimeException(s"Could not submit a run to Databricks. Response: ${response.data.toString()}")
    }
  }

  private[databricks] def waitForSuccessfulFinish(runId: Long): Unit = {
    var runStatus = getRunStatus(runId)

    while (runStatus.isJobPending) {
      log.info(s"Waiting for job to finish: ${runStatus.pretty}.")
      Thread.sleep(JOB_STATUS_SLEEP_MS)

      runStatus = getRunStatus(runId)
    }

    if (runStatus.isFailure) {
      throw new RuntimeException(s"Failed job: ${runStatus.pretty}.")
    }
  }

  private[databricks] def getRunStatus(runId: Long): RunStatusResponse = {
    val url = getRunStatusUrl(baseUrl, runId)
    val response = requests.get(url, auth = auth)

    val responseContent = response.data.toString()
    JsonUtils.fromJson[RunStatusResponse](responseContent)
  }
}

object DatabricksClientImpl {
  private val JOB_STATUS_SLEEP_MS = 5000

  private[databricks] def getCreateFileUrl(baseUrl: String): String = {
    s"$baseUrl/api/2.0/dbfs/put"
  }

  private[databricks] def getRunSubmitUrl(baseUrl: String): String = {
    s"$baseUrl/api/2.1/jobs/runs/submit"
  }

  private[databricks] def getRunStatusUrl(baseUrl: String, runId: Long): String = {
    s"$baseUrl/api/2.1/jobs/runs/get?run_id=${runId.toString}"
  }
}
