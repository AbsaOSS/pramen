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

package za.co.absa.pramen.extras.notification

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.status.{CustomNotification, RuntimeInfo}
import za.co.absa.pramen.api.{DataFormat, PipelineInfo, Query}
import za.co.absa.pramen.extras.mocks.{SimpleHttpClientSpy, TestPrototypes}
import za.co.absa.pramen.extras.notification.EcsPipelineNotificationTarget.{ECS_API_SECRET_KEY, ECS_API_TRUST_SSL_KEY, ECS_API_URL_KEY}
import za.co.absa.pramen.extras.utils.httpclient.SimpleHttpClient

import java.time.Instant

class EcsPipelineNotificationTargetSuite extends AnyWordSpec {
  private val conf = ConfigFactory.parseString(
    s"""
       |$ECS_API_URL_KEY = "https://dummyurl.local"
       |$ECS_API_SECRET_KEY = "abcd"
       |$ECS_API_TRUST_SSL_KEY = true
       |""".stripMargin
  )

  private val dataFormat = DataFormat.Parquet("s3a://dummy_bucket_not_exist/dummy/path", None)
  private val metaTableDef = TestPrototypes.metaTableDef.copy(format = dataFormat)

  "sendNotification" should {
    "send the expected request according to config" in {
      val httpClient = new SimpleHttpClientSpy()

      val notificationTarget = new EcsPipelineNotificationTarget(conf) {
        override protected def getHttpClient(trustAllSslCerts: Boolean): SimpleHttpClient = httpClient
      }

      val task1 = TestPrototypes.taskNotification.copy(outputTable = metaTableDef)
      val dataFormat2 = DataFormat.Parquet("s3a://dummy_bucket_not_exist/dummy/path2", None)
      val metaTableDef2 = TestPrototypes.metaTableDef.copy(name = "table2", format = dataFormat2)
      val task2 = TestPrototypes.taskNotification.copy(jobName = "Job 3", outputTable = metaTableDef2)
      val dataFormat3 = DataFormat.Delta(Query.Table("table2"), None)
      val metaTableDef3 = TestPrototypes.metaTableDef.copy(name = "table3", format = dataFormat3)
      val task3 = TestPrototypes.taskNotification.copy(jobName = "Job 3", outputTable = metaTableDef3)

      notificationTarget.sendNotification(
        PipelineInfo("Dummy", "DEV", RuntimeInfo(), Instant.now, None, None, None, Seq.empty, "pid_123", None),
        Seq(task1, task2, task3),
        CustomNotification(Seq.empty, Seq.empty)
      )

      assert(httpClient.executeCalled == 2)
      assert(httpClient.requests.head.url == "https://dummyurl.local/kk")
      assert(httpClient.requests.head.body.contains("""{"ecs_path":"/dummy/path/pramen_info_date=2022-02-18"}"""))
      assert(httpClient.requests.head.headers("x-api-key") == "abcd")
      assert(httpClient.requests(1).url == "https://dummyurl.local/kk")
      assert(httpClient.requests(1).body.contains("""{"ecs_path":"/dummy/path2/pramen_info_date=2022-02-18"}"""))
      assert(httpClient.requests(1).headers("x-api-key") == "abcd")
    }
  }

  "getEcsDetails" should {
    "get parameters from config" in {
      val (url, key, trust) = EcsPipelineNotificationTarget.getEcsDetails(conf)

      assert(url == "https://dummyurl.local")
      assert(key == "abcd")
      assert(trust)
    }
  }
}
