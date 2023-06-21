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

import scala.util.{Properties, Random}

class DatabricksClientImplSuite extends AnyWordSpec {

  "DatabricksClientImpl" should {
    "implement a common interface" in {
      val client = getDummyClient()

      assert(client.isInstanceOf[DatabricksClient])
    }

    "run an integration job on Databricks" ignore {
      val client = getWorkingClient()

      val notebookContents =
         """
           |print("Hello world")
           |""".stripMargin
      val pythonFile = getRandomDBFSFile()
      val job = Map(
        "run_name" -> "Pramen DatabricksClient IntegrationTest",
        "tasks" -> Seq(
          Map(
            "task_key" -> "pramen-py",
            "new_cluster" -> Map(
              "node_type_id" -> "m5d.large",
              "spark_version" -> "12.2.x-scala2.12",
              "num_workers" -> 1
            ),
            "spark_python_task" -> Map(
              "python_file" -> pythonFile
            )
          )
        )
      )

      client.createFile(notebookContents, pythonFile)
      client.runTransientJob(job)
    }

    "construct an url for creating a file" in {
      val url = DatabricksClientImpl.getCreateFileUrl("https://example.org")
      val expectedUrl = "https://example.org/api/2.0/dbfs/put"

      assert(url == expectedUrl)
    }

    "construct an url for submitting jobs" in {
      val url = DatabricksClientImpl.getRunSubmitUrl("https://example.org")
      val expectedUrl = "https://example.org/api/2.1/jobs/runs/submit"

      assert(url == expectedUrl)
    }

    "construct an url for getting a run status" in {
      val url = DatabricksClientImpl.getRunStatusUrl("https://example.org", 10000)
      val expectedUrl = "https://example.org/api/2.1/jobs/runs/get?run_id=10000"

      assert(url == expectedUrl)
    }
  }
  private def getRandomString(): String = {
    Random.nextInt(10000).toString
  }

  private def getRandomDBFSFile(): String = {
    s"dbfs:/tmp/pramen-dbx-test/${getRandomString()}/script.py"
  }

  private def getDummyClient(): DatabricksClientImpl = {
    new DatabricksClientImpl("https://example.org/", "[token]")
  }

  private def getWorkingClient(): DatabricksClientImpl = {
    val host = Properties.envOrNone("DATABRICKS_HOST").get
    val token = Properties.envOrNone("DATABRICKS_TOKEN").get

    new DatabricksClientImpl(host, token)
  }
}
