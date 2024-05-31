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
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost, HttpPut}
import org.apache.http.util.EntityUtils
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.mocks.HttpClientSpy
import za.co.absa.pramen.extras.utils.httpclient.impl.{BasicHttpClient, HttpDeleteWithBody, HttpGetWithBody}

import java.net.URI

class BasicHttpClientSuite extends AnyWordSpec {
  "execute" should {
    "pass a proper request to the underlying HTTP client" in {
      val (basicHttpClient, httpClientSpy) = getHttpClientSpy()

      val request = SimpleHttpRequest("http://dummy.abc", HttpMethod.GET)

      val response = basicHttpClient.execute(request)

      assert(httpClientSpy.doExecuteCalled == 1)
      assert(response.statusCode == HttpStatus.SC_OK)
    }
  }

  "close" should {
    "invoke the underlying close method of the http client" in {
      val (basicHttpClient, httpClientSpy) = getHttpClientSpy()

      basicHttpClient.close()

      assert(httpClientSpy.closeCalled == 1)
    }
  }

  "getHttpClient" should {
    "return the underlying http client" in {
      assert(BasicHttpClient().getHttpClient != null)
      assert(BasicHttpClient(true).getHttpClient != null)
    }
  }

  "getApacheHttpRequest" should {
    "support GET" in {
      val request = SimpleHttpRequest("dummy", HttpMethod.GET)

      val httpRequest = BasicHttpClient.getApacheHttpRequest(request)

      assert(httpRequest.isInstanceOf[HttpGet])
      assert(httpRequest.asInstanceOf[HttpGet].getURI == URI.create("dummy"))
    }

    "support GET with body" in {
      val request = SimpleHttpRequest("dummy", HttpMethod.GET, Map.empty, Some("body"))

      val httpRequest = BasicHttpClient.getApacheHttpRequest(request)

      assert(httpRequest.isInstanceOf[HttpGetWithBody])
      assert(httpRequest.asInstanceOf[HttpGetWithBody].getURI == URI.create("dummy"))
      assert(EntityUtils.toString(httpRequest.asInstanceOf[HttpGetWithBody].getEntity) == "body")
    }

    "support PUT" in {
      val request = SimpleHttpRequest("dummy", HttpMethod.PUT)

      val httpRequest = BasicHttpClient.getApacheHttpRequest(request)

      assert(httpRequest.isInstanceOf[HttpPut])
      assert(httpRequest.asInstanceOf[HttpPut].getURI == URI.create("dummy"))
    }

    "support POST" in {
      val request = SimpleHttpRequest("dummy", HttpMethod.POST, Map.empty, Some("body"))

      val httpRequest = BasicHttpClient.getApacheHttpRequest(request)

      assert(httpRequest.isInstanceOf[HttpPost])
      assert(httpRequest.asInstanceOf[HttpPost].getURI == URI.create("dummy"))
      assert(EntityUtils.toString(httpRequest.asInstanceOf[HttpPost].getEntity) == "body")
    }

    "support DELETE" in {
      val request = SimpleHttpRequest("dummy", HttpMethod.DELETE)

      val httpRequest = BasicHttpClient.getApacheHttpRequest(request)

      assert(httpRequest.isInstanceOf[HttpDelete])
      assert(httpRequest.asInstanceOf[HttpDelete].getURI == URI.create("dummy"))
    }

    "support DELETE with body" in {
      val request = SimpleHttpRequest("dummy", HttpMethod.DELETE, Map.empty, Some("body"))

      val httpRequest = BasicHttpClient.getApacheHttpRequest(request)

      assert(httpRequest.isInstanceOf[HttpDeleteWithBody])
      assert(httpRequest.asInstanceOf[HttpDeleteWithBody].getURI == URI.create("dummy"))
      assert(EntityUtils.toString(httpRequest.asInstanceOf[HttpDeleteWithBody].getEntity) == "body")
    }
  }

  def getHttpClientSpy(trustAllCerts: Boolean = false): (SimpleHttpClient, HttpClientSpy) = {
    val httpClientSpy = new HttpClientSpy()
    val basicHttpClient = new BasicHttpClient(trustAllCerts) {
      override private[extras] def getHttpClient = httpClientSpy
    }
    (basicHttpClient, httpClientSpy)
  }
}
