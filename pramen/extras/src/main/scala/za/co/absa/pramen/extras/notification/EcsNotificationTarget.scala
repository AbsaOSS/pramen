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

import com.typesafe.config.Config
import org.apache.http.HttpStatus
import org.apache.http.client.HttpClient
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.{ConnectionSocketFactory, PlainConnectionSocketFactory}
import org.apache.http.conn.ssl.{NoopHostnameVerifier, SSLConnectionSocketFactory}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.BasicHttpClientConnectionManager
import org.apache.http.ssl.{SSLContexts, TrustStrategy}
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{NotificationTarget, TaskNotification}
import za.co.absa.pramen.core.utils.Emoji
import za.co.absa.pramen.extras.sink.HttpDeleteWithBody

import java.security.cert.X509Certificate

/**
  * Runs the ECS cleanup API against the target partition after the job jas completed.
  */
class EcsNotificationTarget(conf: Config) extends NotificationTarget {
  override def config: Config = conf

  override def sendNotification(notification: TaskNotification): Unit = {
    // ToDo implementation
  }
}

object EcsNotificationTarget {
  private val log = LoggerFactory.getLogger(this.getClass)

  def cleanUpS3VersionsForPath(partitionPath: String,
                               apiUrl: String,
                               apiKey: String,
                               httpClient: HttpClient): Unit = {
    val body = s"""{"ecs_path":"$partitionPath"}"""
    log.info(s"Sending: $body")

    val httpDelete = new HttpDeleteWithBody(apiUrl)

    httpDelete.addHeader("x-api-key", apiKey)
    httpDelete.setEntity(new StringEntity(body))

    try {
      val response = httpClient.execute(httpDelete)
      val statusCode = response.getStatusLine.getStatusCode
      val responseBody = EntityUtils.toString(response.getEntity)

      if (statusCode != HttpStatus.SC_OK) {
        log.error(s"${Emoji.FAILURE} Failed to clean up S3 versions for $partitionPath. Response: $statusCode $responseBody")
      } else {
        log.info(s"${Emoji.SUCCESS} S3 versions cleanup for $partitionPath was successful. Response: $responseBody")
      }
    } catch {
      case ex: Throwable =>
        log.error(s"${Emoji.FAILURE} Unable to call the cleanup API via URL: $apiUrl.", ex)
    }
  }

  private[extras] def getHttpClient(trustAllSslCerts: Boolean): CloseableHttpClient = {
    // Using Apache HTTP Client.
    // Tried using com.lihaoyi:requests:0.8.0,
    // but for some strange reason the EnceladusSink class can't be found/loaded
    // when this library is used.

    if (trustAllSslCerts) {
      log.warn("Trusting all SSL certificates for the cleanup API.")
      val trustStrategy = new TrustStrategy {
        override def isTrusted(x509Certificates: Array[X509Certificate], s: String): Boolean = true
      }

      val sslContext = SSLContexts.custom.loadTrustMaterial(null, trustStrategy).build
      val sslsf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE)

      val socketFactoryRegistry =
        RegistryBuilder.create[ConnectionSocketFactory]()
          .register("https", sslsf)
          .register("http", new PlainConnectionSocketFactory())
          .build()

      val connectionManager = new BasicHttpClientConnectionManager(socketFactoryRegistry)

      HttpClients.custom()
        .setSSLSocketFactory(sslsf)
        .setConnectionManager(connectionManager)
        .build()
    } else {
      HttpClients.createDefault()
    }
  }
}
