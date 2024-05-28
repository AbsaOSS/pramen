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
import org.apache.http.client.HttpClient
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.{HttpEntity, HttpResponse, HttpStatus, StatusLine}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when => whenMock}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.notification.EcsNotificationTarget.{ECS_API_KEY_KEY, ECS_API_TRUST_SSL_KEY, ECS_API_URL_KEY}
import za.co.absa.pramen.extras.sink.HttpDeleteWithBody

class EcsNotificationTargetSuite extends AnyWordSpec {
  "sendNotification" should {
    "send the expected request according to config" in {

    }
  }

  "getEcsDetails" should {
    "get parameters from config" in {
      val conf = ConfigFactory.parseString(
        s"""
          |$ECS_API_URL_KEY = "https://dummyurl.local"
          |$ECS_API_KEY_KEY = "abcd"
          |$ECS_API_TRUST_SSL_KEY = true
          |""".stripMargin
      )

      val (url, key, trust) = new EcsNotificationTarget(conf).getEcsDetails

      assert(url == "https://dummyurl.local")
      assert(key == "abcd")
      assert(trust)
    }
  }

  "cleanUpS3VersionsForPath" should {
    "send the expected request according to config" in {
      val httpClient = mock(classOf[HttpClient])
      val httpResponse = mock(classOf[HttpResponse])
      val statusLine = mock(classOf[StatusLine])
      val httpEntity = mock(classOf[HttpEntity])

      whenMock(statusLine.getStatusCode).thenReturn(HttpStatus.SC_OK)
      whenMock(httpResponse.getStatusLine).thenReturn(statusLine)
      whenMock(httpResponse.getEntity).thenReturn(httpEntity)
      whenMock(httpClient.execute(any[HttpDeleteWithBody])).thenReturn(httpResponse)

      EcsNotificationTarget.cleanUpS3VersionsForPath(new Path("bucket/path/date=2024-02-18"), "https://dummyurl.local", "abcd", httpClient)
    }
  }

  "getHttpClient" should {
    "return a standard HTTP client when not trusting all SSL certificates blindly" in {
      val httpClient = EcsNotificationTarget.getHttpClient(false)

      assert(httpClient.isInstanceOf[CloseableHttpClient])
    }

    "return a custom HTTP client when trusting all SSL certificates" in {
      val httpClient = EcsNotificationTarget.getHttpClient(true)

      assert(httpClient.isInstanceOf[CloseableHttpClient])
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
      val request = EcsNotificationTarget.getCleanUpS3VersionsRequest("{\"ecs_path\":\"bucket/path/date=2024-02-18\"}", "https://dummyurl.local", "abcd")

      assert(request.isInstanceOf[HttpDeleteWithBody])
      assert(request.getHeaders("x-api-key").head.getValue == "abcd")
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
