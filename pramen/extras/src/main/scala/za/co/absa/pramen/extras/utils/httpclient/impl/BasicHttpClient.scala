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

package za.co.absa.pramen.extras.utils.httpclient.impl

import org.apache.http.client.methods._
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.{ConnectionSocketFactory, PlainConnectionSocketFactory}
import org.apache.http.conn.ssl.{NoopHostnameVerifier, SSLConnectionSocketFactory}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.BasicHttpClientConnectionManager
import org.apache.http.ssl.{SSLContexts, TrustStrategy}
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory
import za.co.absa.pramen.extras.utils.httpclient.HttpMethod.{DELETE, GET, POST, PUT}
import za.co.absa.pramen.extras.utils.httpclient.{SimpleHttpClient, SimpleHttpRequest, SimpleHttpResponse}

import java.security.cert.X509Certificate

class BasicHttpClient(val trustAllSslCerts: Boolean) extends SimpleHttpClient {
  import BasicHttpClient._

  private val log = LoggerFactory.getLogger(this.getClass)
  private val httpClient = getHttpClient

  override def execute(request: SimpleHttpRequest): SimpleHttpResponse = {
    val httpRequest = getApacheHttpRequest(request)

    val response = httpClient.execute(httpRequest)
    val bodyStr = EntityUtils.toString(response.getEntity)
    val body = if (bodyStr.isEmpty) None else Option(bodyStr)

    SimpleHttpResponse(
      response.getStatusLine.getStatusCode,
      body,
      Seq.empty
    )
  }

  override def close(): Unit =
    httpClient.close()

  private[extras] def getHttpClient: CloseableHttpClient =
    if (trustAllSslCerts) {
      log.warn("Trusting all SSL certificates for the cleanup API.")
      val sslsf = getTrustingSocketFactory

      val socketFactoryRegistry =
        RegistryBuilder
          .create[ConnectionSocketFactory]()
          .register("https", sslsf)
          .register("http", new PlainConnectionSocketFactory())
          .build()

      val connectionManager = new BasicHttpClientConnectionManager(socketFactoryRegistry)

      HttpClients
        .custom()
        .setSSLSocketFactory(sslsf)
        .setConnectionManager(connectionManager)
        .build()
    } else {
      HttpClients.createDefault()
    }
}

object BasicHttpClient {
  def apply(trustAllSslCerts: Boolean = false): BasicHttpClient =
    new BasicHttpClient(trustAllSslCerts)

  /**
    *  Returns a socket factory that trusts all SSL certificates.
    */
  private[extras] def getTrustingSocketFactory: SSLConnectionSocketFactory = {
    val trustStrategy = new TrustStrategy {
      override def isTrusted(x509Certificates: Array[X509Certificate], s: String): Boolean = true
    }

    val sslContext = SSLContexts.custom.loadTrustMaterial(null, trustStrategy).build
    new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE)
  }

  private[extras] def getApacheHttpRequest(request: SimpleHttpRequest): HttpUriRequest = {
    val httpRequest = request.method match {
      case GET =>
        request.body match {
          case Some(body) =>
            val req = new HttpGetWithBody(request.url)
            req.setEntity(new StringEntity(body))
            req
          case None => new HttpGet(request.url)
        }

      case POST =>
        val req = new HttpPost(request.url)
        req.setEntity(new org.apache.http.entity.StringEntity(request.body.getOrElse("")))
        req

      case PUT =>
        val req = new HttpPut(request.url)
        req.setEntity(new org.apache.http.entity.StringEntity(request.body.getOrElse("")))
        req
      case DELETE =>
        request.body match {
          case Some(body) =>
            val req = new HttpDeleteWithBody(request.url)
            req.setEntity(new StringEntity(body))
            req
          case None => new HttpDelete(request.url)
        }
      case _ => throw new IllegalArgumentException(s"Unsupported HTTP method: ${request.method}")
    }

    request.headers.foreach { case (k, v) => httpRequest.addHeader(k, v) }

    httpRequest
  }
}
