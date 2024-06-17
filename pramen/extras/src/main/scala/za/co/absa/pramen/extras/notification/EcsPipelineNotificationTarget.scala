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
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.status.{CustomNotification, RunStatus}
import za.co.absa.pramen.api.{DataFormat, PipelineInfo, PipelineNotificationTarget, TaskNotification}
import za.co.absa.pramen.extras.utils.ConfigUtils
import za.co.absa.pramen.extras.utils.httpclient.SimpleHttpClient

/**
  * Runs the ECS cleanup API against the target partition after the job jas completed.
  *
  * Example usage:
  * {{{
  *   pramen.ecs.api {
  *      url = "https://dummy.local"
  *      secret = "aabbcc"
  *      trust.all.ssl.certificates = false
  *   }
  *
  *   pramen.pipeline.notification.targets = [ "za.co.absa.pramen.extras.notification.EcsPipelineNotificationTarget" ]
  * }}}
  */
class EcsPipelineNotificationTarget(conf: Config) extends PipelineNotificationTarget {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def config: Config = conf

  /** Sends a notification after completion of the pipeline. */
  override def sendNotification(pipelineInfo: PipelineInfo,
                                tasksCompleted: Seq[TaskNotification],
                                customNotification: CustomNotification): Unit = {
    log.info(s"Running the ECS cleanup pipeline notification target...")
    val (ecsApiUrl, ecsApiKey, trustAllSslCerts) = EcsPipelineNotificationTarget.getEcsDetails(conf)

    val httpClient = getHttpClient(trustAllSslCerts)

    try {
      tasksCompleted.foreach { task =>
        task.runInfo match {
          case Some(runInfo) if task.status.isInstanceOf[RunStatus.Succeeded] =>
            if (!task.tableDef.format.isTransient &&
              !task.tableDef.format.isInstanceOf[DataFormat.Null] &&
              !task.tableDef.format.isInstanceOf[DataFormat.Raw]) {
              EcsNotificationTarget.cleanUpS3VersionsForTable(task.tableDef, runInfo.infoDate, ecsApiUrl, ecsApiKey, httpClient)
            } else {
              log.info(s"The task outputting to '${task.tableName}' for '${runInfo.infoDate}' outputs to ${task.tableDef.format.name} format - skipping ECS cleanup...")
            }
          case Some(runInfo) =>
            log.info(s"The task outputting to '${task.tableName}' for '${runInfo.infoDate}' status is not a success - skipping ECS cleanup...")
          case None =>
            log.info(s"The task outputting to '${task.tableName}' status is not a success - skipping ECS cleanup...")
        }
      }
    } finally {
      httpClient.close()
    }
  }

  protected def getHttpClient(trustAllSslCerts: Boolean): SimpleHttpClient = {
    EcsNotificationTarget.getHttpClient(trustAllSslCerts)
  }
}

object EcsPipelineNotificationTarget {
  val ECS_API_URL_KEY = "pramen.ecs.api.url"
  val ECS_API_SECRET_KEY = "pramen.ecs.api.secret"
  val ECS_API_TRUST_SSL_KEY = "pramen.ecs.api.trust.all.ssl.certificates"

  private[extras] def getEcsDetails(conf: Config): (String, String, Boolean) = {
    require(conf.hasPath(ECS_API_URL_KEY), s"The key is not defined: '$ECS_API_URL_KEY'")
    require(conf.hasPath(ECS_API_SECRET_KEY), s"The key is not defined: '$ECS_API_SECRET_KEY'")

    val ecsApiUrl = conf.getString(ECS_API_URL_KEY)
    val ecsApiKey = conf.getString(ECS_API_SECRET_KEY)
    val trustAllSslCerts = ConfigUtils.getOptionBoolean(conf, ECS_API_TRUST_SSL_KEY).getOrElse(false)

    (ecsApiUrl, ecsApiKey, trustAllSslCerts)
  }

}
