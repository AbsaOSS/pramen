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
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.status.TaskResult
import za.co.absa.pramen.api.{DataFormat, MetaTableDef, NotificationTarget, PipelineInfo}
import za.co.absa.pramen.core.utils.Emoji
import za.co.absa.pramen.extras.utils.ConfigUtils
import za.co.absa.pramen.extras.utils.httpclient.{HttpMethod, SimpleHttpClient, SimpleHttpRequest}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
  * Runs the ECS cleanup API against the target partition after the job jas completed.
  */
class EcsNotificationTarget(conf: Config) extends NotificationTarget {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def config: Config = conf

  override def sendNotification(pipelineInfo: PipelineInfo, notification: TaskResult): Unit = {
    if (notification.runInfo.isEmpty) {
      log.warn(s"Information date not provided - skipping ECS cleanup.")
      return
    }

    val (ecsApiUrl, ecsApiKey, trustAllSslCerts) = EcsNotificationTarget.getEcsDetails(conf)
    val tableDef = notification.outputTable
    val httpClient = getHttpClient(trustAllSslCerts)

    try {
      EcsNotificationTarget.cleanUpS3VersionsForTable(tableDef, notification.runInfo.get.infoDate, ecsApiUrl, ecsApiKey, httpClient)
    } finally {
      httpClient.close()
    }
  }

  protected def getHttpClient(trustAllSslCerts: Boolean): SimpleHttpClient = {
    EcsNotificationTarget.getHttpClient(trustAllSslCerts)
  }
}

object EcsNotificationTarget {
  private val log = LoggerFactory.getLogger(this.getClass)

  val ECS_API_URL_KEY = "ecs.api.url"
  val ECS_API_SECRET_KEY = "ecs.api.key"
  val ECS_API_TRUST_SSL_KEY = "ecs.api.trust.all.ssl.certificates"

  val ECS_PREFIXES: Seq[String] = Seq("s3a://")

  /**
    * Cleans up a Pramen metatable via a special REST API call.
    */
  def cleanUpS3VersionsForTable(tableDef: MetaTableDef,
                                infoDate: LocalDate,
                                apiUrl: String,
                                apiKey: String,
                                httpClient: SimpleHttpClient): Unit = {
    log.info(s"Running the ECS cleanup notification target: $apiUrl, metatable=${tableDef.name}, format=${tableDef.format.name}, " +
      s"Info date column=${tableDef.infoDateColumn}, Info date format=${tableDef.infoDateFormat}, Info date=$infoDate")

    val formatter = DateTimeFormatter.ofPattern(tableDef.infoDateFormat)
    val infoDateStr = formatter.format(infoDate)

    tableDef.format match {
      case DataFormat.Parquet(basePath, _) =>
        log.info(s"Base path: $basePath")
        val basePathLowerCase = basePath.toLowerCase
        if (!ECS_PREFIXES.exists(prefix => basePathLowerCase.startsWith(prefix))) {
          log.info(s"The base bath ($basePath) is not on S3. S3 versions cleanup won't be done.")
          return
        }

        val partitionPath = new Path(basePath, s"${tableDef.infoDateColumn}=$infoDateStr")
        log.info(s"Partition path: $partitionPath")

        EcsNotificationTarget.cleanUpS3VersionsForPath(partitionPath, apiUrl, apiKey, httpClient)
      case format =>
        log.warn(s"Format ${format.name} is not supported. Skipping cleanup.")
    }
  }

  /**
    * Cleans up an ECS path via a special REST API call.
    */
  def cleanUpS3VersionsForPath(partitionPath: Path,
                               apiUrl: String,
                               apiKey: String,
                               httpClient: SimpleHttpClient): Unit = {
    val body = getCleanUpS3VersionsRequestBody(partitionPath)
    log.info(s"Sending: $body")

    val httpDelete = getCleanUpS3VersionsRequest(body, apiUrl, apiKey)

    try {
      val response = httpClient.execute(httpDelete)
      val statusCode = response.statusCode
      val responseBody = response.body.getOrElse("")

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
    * Returns an instance of an HTTP client.
    *
    * Do not forget to close the client after use.
    *
    * @param trustAllSslCerts if true, the client will trust any SSL certificate.
    * @return an Http Client
    */
  def getHttpClient(trustAllSslCerts: Boolean): SimpleHttpClient = {
    log.info(s"Trust all SSL certificates: $trustAllSslCerts")
    SimpleHttpClient(trustAllSslCerts)
  }

  private[extras] def getEcsDetails(conf: Config): (String, String, Boolean) = {
    require(conf.hasPath(ECS_API_URL_KEY), s"The key is not defined: '$ECS_API_URL_KEY'")
    require(conf.hasPath(ECS_API_SECRET_KEY), s"The key is not defined: '$ECS_API_SECRET_KEY'")

    val ecsApiUrl = conf.getString(ECS_API_URL_KEY)
    val ecsApiKey = conf.getString(ECS_API_SECRET_KEY)
    val trustAllSslCerts = ConfigUtils.getOptionBoolean(conf, ECS_API_TRUST_SSL_KEY).getOrElse(false)

    (ecsApiUrl, ecsApiKey, trustAllSslCerts)
  }

  private[extras] def getCleanUpS3VersionsRequestBody(partitionPath: Path): String = {
    val partitionPathWithoutAuthority = removeAuthority(partitionPath)
    s"""{"ecs_path":"$partitionPathWithoutAuthority"}"""
  }

  private[extras] def getCleanUpS3VersionsRequest(requestBody: String, apiUrl: String, apiKey: String): SimpleHttpRequest = {
    val effectiveUrl = if (apiUrl.endsWith("/kk")) {
      apiUrl
    } else {
      s"$apiUrl/kk"
    }

    SimpleHttpRequest(
      effectiveUrl,
      HttpMethod.DELETE,
      Map("x-api-key" -> apiKey),
      Some(requestBody)
    )
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
