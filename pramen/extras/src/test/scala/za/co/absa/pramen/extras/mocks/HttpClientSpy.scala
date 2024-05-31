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

package za.co.absa.pramen.extras.mocks

import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.entity.StringEntity
import org.apache.http.impl.DefaultHttpResponseFactory
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.message.BasicStatusLine
import org.apache.http.params.HttpParams
import org.apache.http.protocol.HttpContext
import org.apache.http.{HttpHost, HttpRequest, HttpStatus, HttpVersion}

class HttpClientSpy(response: CloseableHttpResponse = HttpClientSpy.getDummyResponse()) extends CloseableHttpClient {
  var doExecuteCalled = 0
  var closeCalled = 0
  var lastTarget: HttpHost = _
  var lastRequest: HttpRequest = _
  var lastContext: HttpContext = _

  override def doExecute(target: HttpHost, request: HttpRequest, context: HttpContext): CloseableHttpResponse = {
    doExecuteCalled += 1
    lastTarget = target
    lastRequest = request
    lastContext = context

    response
  }

  override def close(): Unit = closeCalled += 1

  override def getParams: HttpParams = null

  override def getConnectionManager: ClientConnectionManager = null
}

object HttpClientSpy {
  def getDummyResponse(body: String = ""): CloseableHttpResponse = {
    val response = new DefaultHttpResponseFactory()
      .newHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, null), null)

    response.setEntity(new StringEntity(body))

    new CloseableHttpResponseAdapterMock(response)
  }
}
