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
import org.apache.hadoop.fs.Path
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
import za.co.absa.pramen.api.{DataFormat, NotificationTarget, TaskNotification}
import za.co.absa.pramen.core.utils.Emoji
import za.co.absa.pramen.extras.notification.EcsNotificationTarget.{ECS_API_KEY_KEY, ECS_API_TRUST_SSL_KEY, ECS_API_URL_KEY}
import za.co.absa.pramen.extras.sink.HttpDeleteWithBody
import za.co.absa.pramen.extras.utils.ConfigUtils

import java.security.cert.X509Certificate
import java.time.format.DateTimeFormatter

/**
  * Runs the ECS cleanup API against the target partition after the job jas completed.
  */
class EcsNotificationTarget(conf: Config) extends NotificationTarget {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def config: Config = conf

  override def sendNotification(notification: TaskNotification): Unit = {
    if (notification.infoDate.isEmpty) {
      log.warn(s"Information date not provided - skipping ECS cleanup.")
      return
    }

    val (ecsApiUrl, ecsApiKey, trustAllSslCerts) = getEcsDetails
    val tableDef = notification.tableDef

    log.info(s"ECS API URL: $ecsApiUrl")
    log.info(s"ECS API Key: [redacted]")
    log.info(s"Trust all SSL certificates: $trustAllSslCerts")
    log.info(s"Metatable: ${tableDef.name} (${tableDef.format.name})")
    log.info(s"Info date column: ${tableDef.infoDateColumn}")
    log.info(s"Info date format: ${tableDef.infoDateFormat}")

    val formatter = DateTimeFormatter.ofPattern(tableDef.infoDateFormat)
    val infoDateStr = formatter.format(notification.infoDate.get)
    log.info(s"Info date: $infoDateStr")

    tableDef.format match {
      case DataFormat.Parquet(basePath, _) =>
        log.info(s"Base path: $basePath")
        if (!basePath.toLowerCase.startsWith("s3://") && !basePath.toLowerCase.startsWith("s3a://")) {
          log.warn(s"The base bath ($basePath) is not on S3. S3 versions cleanup won't be done.")
          return
        }

        val partitionPath = new Path(basePath, s"${tableDef.infoDateColumn}=$infoDateStr")
        log.info(s"Partition path: $partitionPath")

        val httpClient = getHttpClient(trustAllSslCerts)
        EcsNotificationTarget.cleanUpS3VersionsForPath(partitionPath, ecsApiUrl, ecsApiKey, httpClient)
      case format =>
        log.warn(s"Format ${format.name} is not supported. Skipping cleanup.")
    }
  }

  protected def getHttpClient(trustAllSslCerts: Boolean): CloseableHttpClient = {
    EcsNotificationTarget.getHttpClient(trustAllSslCerts)
  }

  private[extras] def getEcsDetails: (String, String, Boolean) = {
    require(conf.hasPath(ECS_API_URL_KEY), s"The key is not defined: '$ECS_API_URL_KEY'")
    require(conf.hasPath(ECS_API_KEY_KEY), s"The key is not defined: '$ECS_API_KEY_KEY'")

    val ecsApiUrl = conf.getString(ECS_API_URL_KEY)
    val ecsApiKey = conf.getString(ECS_API_KEY_KEY)
    val trustAllSslCerts = ConfigUtils.getOptionBoolean(conf, ECS_API_TRUST_SSL_KEY).getOrElse(false)

    (ecsApiUrl, ecsApiKey, trustAllSslCerts)
  }
}

object EcsNotificationTarget {
  private val log = LoggerFactory.getLogger(this.getClass)

  val ECS_API_URL_KEY = "ecs.api.url"
  val ECS_API_KEY_KEY = "ecs.api.key"
  val ECS_API_TRUST_SSL_KEY = "ecs.api.trust.all.ssl.certificates"

  /**
    * Cleans up ECS buckets via a special REST API call.
    */
  def cleanUpS3VersionsForPath(partitionPath: Path,
                               apiUrl: String,
                               apiKey: String,
                               httpClient: HttpClient): Unit = {
    val body = getCleanUpS3VersionsRequestBody(partitionPath)
    log.info(s"Sending: $body")

    val httpDelete = getCleanUpS3VersionsRequest(body, apiUrl, apiKey)

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

  /**
    * Returns an instance of Apache HTTP client.
    *
    * Do not forget to close the client after use.
    *
    * @param trustAllSslCerts if true, the client will trust any SSL certificate.
    * @return an Http Client
    */
  def getHttpClient(trustAllSslCerts: Boolean): CloseableHttpClient = {
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

  private[extras] def getCleanUpS3VersionsRequestBody(partitionPath: Path): String = {
    val partitionPathWithoutAuthority = removeAuthority(partitionPath)
    s"""{"ecs_path":"$partitionPathWithoutAuthority"}"""
  }

  private[extras] def getCleanUpS3VersionsRequest(requestBody: String, apiUrl: String, apiKey: String): HttpDeleteWithBody = {
    val httpDelete = new HttpDeleteWithBody(apiUrl)

    httpDelete.addHeader("x-api-key", apiKey)
    httpDelete.setEntity(new StringEntity(requestBody))

    httpDelete
  }

  private[extras] def removeAuthority(path: Path): String = {
    val uri = path.toUri
    if (uri.getHost != null) {
      s"${uri.getHost}${uri.getPath}"
    } else {
      s"${uri.getPath}"
    }
  }
}
