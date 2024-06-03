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

package za.co.absa.pramen.extras.utils.httpclient

import org.apache.http.HttpStatus
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.mocks.SimpleHttpClientSpy
import za.co.absa.pramen.extras.utils.httpclient.impl.RetryableHttpClient

class RetryableHttpClientSuite extends AnyWordSpec {
  private val request = SimpleHttpRequest("https://dummy.abc", HttpMethod.POST)

  "execute" should {
    "return the response if the first attempt succeeds" in {
      val spy = new SimpleHttpClientSpy()
      val client: SimpleHttpClient = new RetryableHttpClient(spy, 3, 1000)

      val response = client.execute(request)

      assert(response.statusCode == HttpStatus.SC_OK)
      assert(response.body.contains("body"))
      assert(spy.executeCalled == 1)
    }

    "return the response if the first attempt fails, but next one succeeds" in {
      val spy = new SimpleHttpClientSpy(failNTimes = 1)
      val client: SimpleHttpClient = new RetryableHttpClient(spy, 3, 1000)

      val response = client.execute(request)

      assert(response.statusCode == HttpStatus.SC_OK)
      assert(response.body.contains("body"))
      assert(spy.executeCalled == 2)
    }

    "return the response if the first attempt returns an error status code, but next one succeeds" in {
      val failResp = SimpleHttpResponse(HttpStatus.SC_BAD_REQUEST, Some("body"), Seq.empty)
      val spy = new SimpleHttpClientSpy(failResponse = Option(failResp), failNTimes = 1)
      val client: SimpleHttpClient = new RetryableHttpClient(spy, 3, 1000)

      val response = client.execute(request)

      assert(response.statusCode == HttpStatus.SC_OK)
      assert(response.body.contains("body"))
      assert(spy.executeCalled == 2)
    }

    "throw an exception if out of attempts" in {
      val spy = new SimpleHttpClientSpy(failNTimes = 3)
      val client: SimpleHttpClient = new RetryableHttpClient(spy, 3, 1000)

      val ex = intercept[RuntimeException] {
        client.execute(request)
      }

      assert(ex.getMessage == "Test Exception")

      assert(spy.executeCalled == 3)
    }
  }

  "close" should {
    "call super.close" in {
      val spy = new SimpleHttpClientSpy()
      val client: SimpleHttpClient = new RetryableHttpClient(spy, 3, 1000)

      client.close()

      assert(spy.closeCalled == 1)
    }
  }
}
