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

import org.apache.http._
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.params.HttpParams

import java.util.Locale

class CloseableHttpResponseAdapterMock(response: HttpResponse) extends CloseableHttpResponse {
  override def getStatusLine: StatusLine = response.getStatusLine

  override def setStatusLine(statusline: StatusLine): Unit = ???

  override def setStatusLine(ver: ProtocolVersion, code: Int): Unit = ???

  override def setStatusLine(ver: ProtocolVersion, code: Int, reason: String): Unit = ???

  override def setStatusCode(code: Int): Unit = ???

  override def setReasonPhrase(reason: String): Unit = ???

  override def getEntity: HttpEntity = response.getEntity

  override def setEntity(entity: HttpEntity): Unit = ???

  override def getLocale: Locale = response.getLocale

  override def setLocale(loc: Locale): Unit = ???

  override def getProtocolVersion: ProtocolVersion = response.getProtocolVersion

  override def containsHeader(name: String): Boolean = ???

  override def getHeaders(name: String): Array[Header] = response.getHeaders(name)

  override def getFirstHeader(name: String): Header = response.getFirstHeader(name)

  override def getLastHeader(name: String): Header = response.getLastHeader(name)

  override def getAllHeaders: Array[Header] = response.getAllHeaders

  override def addHeader(header: Header): Unit = ???

  override def addHeader(name: String, value: String): Unit = ???

  override def setHeader(header: Header): Unit = ???

  override def setHeader(name: String, value: String): Unit = ???

  override def setHeaders(headers: Array[Header]): Unit = ???

  override def removeHeader(header: Header): Unit = ???

  override def removeHeaders(name: String): Unit = ???

  override def headerIterator(): HeaderIterator = ???

  override def headerIterator(name: String): HeaderIterator = ???

  override def getParams: HttpParams = response.getParams

  override def setParams(params: HttpParams): Unit = ???

  override def close(): Unit = {}
}
