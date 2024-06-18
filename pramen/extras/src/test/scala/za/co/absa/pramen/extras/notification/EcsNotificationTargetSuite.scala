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
import org.apache.hadoop.fs.Path
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.DataFormat
import za.co.absa.pramen.extras.mocks.{SimpleHttpClientSpy, TestPrototypes}
import za.co.absa.pramen.extras.notification.EcsNotificationTarget.{ECS_API_SECRET_KEY, ECS_API_TRUST_SSL_KEY, ECS_API_URL_KEY}
import za.co.absa.pramen.extras.utils.httpclient.SimpleHttpClient
import za.co.absa.pramen.extras.utils.httpclient.impl.{BasicHttpClient, RetryableHttpClient}

class EcsNotificationTargetSuite extends AnyWordSpec {
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

      val notificationTarget = new EcsNotificationTarget(conf) {
        override protected def getHttpClient(trustAllSslCerts: Boolean): SimpleHttpClient = httpClient
      }

      notificationTarget.sendNotification(null, TestPrototypes.taskNotification.copy(outputTable = metaTableDef))

      assert(httpClient.executeCalled == 1)
      assert(httpClient.requests.head.url == "https://dummyurl.local/kk")
      assert(httpClient.requests.head.body.contains("""{"ecs_path":"/dummy/path/pramen_info_date=2022-02-18"}"""))
      assert(httpClient.requests.head.headers("x-api-key") == "abcd")
    }
  }

  "getEcsDetails" should {
    "get parameters from config" in {
      val (url, key, trust) = EcsNotificationTarget.getEcsDetails(conf)

      assert(url == "https://dummyurl.local")
      assert(key == "abcd")
      assert(trust)
    }
  }

  "cleanUpS3VersionsForPath" should {
    "send the expected request according to config" in {
      val httpClient = new SimpleHttpClientSpy()

      EcsNotificationTarget.cleanUpS3VersionsForPath(new Path("bucket/path/date=2024-02-18"), "https://dummyurl.local", "abcd", httpClient)

      assert(httpClient.executeCalled == 1)
    }
  }

  "getHttpClient" should {
    "return a standard HTTP client when not trusting all SSL certificates blindly" in {
      val httpClient = EcsNotificationTarget.getHttpClient(false)

      assert(httpClient.isInstanceOf[RetryableHttpClient])
      assert(httpClient.asInstanceOf[RetryableHttpClient].baseHttpClient.isInstanceOf[BasicHttpClient])
      assert(!httpClient.asInstanceOf[RetryableHttpClient].baseHttpClient.asInstanceOf[BasicHttpClient].trustAllSslCerts)
    }

    "return a custom HTTP client when trusting all SSL certificates" in {
      val httpClient = EcsNotificationTarget.getHttpClient(true)

      assert(httpClient.isInstanceOf[RetryableHttpClient])
      assert(httpClient.asInstanceOf[RetryableHttpClient].baseHttpClient.isInstanceOf[BasicHttpClient])
      assert(httpClient.asInstanceOf[RetryableHttpClient].baseHttpClient.asInstanceOf[BasicHttpClient].trustAllSslCerts)
    }
  }

  "getCleanUpS3VersionsRequestBody" should {
    "format the API request body properly" in {
      val body = EcsNotificationTarget.getCleanUpS3VersionsRequestBody(new Path("s3a://bucket/path/date=2024-02-18"))

      assert(body == "{\"ecs_path\":\"bucket/path/date=2024-02-18\"}")
    }
  }

  "getCleanUpS3VersionsRequest" should {
    "return the proper request" in {
      val body = "{\"ecs_path\":\"bucket/path/date=2024-02-18\"}"
      val request = EcsNotificationTarget.getCleanUpS3VersionsRequest(body, "https://dummyurl.local/kk", "abcd")

      assert(request.url == "https://dummyurl.local/kk")
      assert(request.headers("x-api-key") == "abcd")
      assert(request.body.contains(body))
    }
  }

  "removeAuthority" should {
    "not change if the path does not contain any" in {
      val path = new Path("/path/date=2024-02-18")
      val actual = EcsNotificationTarget.removeAuthority(path)

      assert(actual == "/path/date=2024-02-18")
    }

    "remove the s3 authority" in {
      val path = new Path("s3://bucket/path/date=2024-02-18")
      val actual = EcsNotificationTarget.removeAuthority(path)

      assert(actual == "bucket/path/date=2024-02-18")
    }

    "remove the s3a authority" in {
      val path = new Path("s3a://bucket/path/date=2024-02-18")
      val actual = EcsNotificationTarget.removeAuthority(path)

      assert(actual == "bucket/path/date=2024-02-18")
    }

    "remove the hdfs authority" in {
      val path = new Path("hdfs://cluster/path/date=2024-02-18")
      val actual = EcsNotificationTarget.removeAuthority(path)

      assert(actual == "cluster/path/date=2024-02-18")
    }
  }
}
