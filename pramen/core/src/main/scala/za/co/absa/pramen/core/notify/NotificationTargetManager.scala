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

package za.co.absa.pramen.core.notify

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api.NotificationTarget
import za.co.absa.pramen.api.status.TaskStatus
import za.co.absa.pramen.core.ExternalChannelFactoryReflect

object NotificationTargetManager {
  val NOTIFICATION_TARGETS_KEY = "pramen.notification.targets"

  /**
    * Returns a notification target by its name.
    *
    * Example:
    * {{{
    *   pramen.notification.targets = [
    *     {
    *       name = "hyperdrive1"
    *       factory.class = "za.co.absa.pramen.core.notify.HyperdriveNotificationTarget"
    *
    *       kafka.topic = "mytopic"
    *
    *       kafka.option {
    *         bootstrap.servers = "dummy:9092,dummy:9093"
    *         sasl.mechanism = "GSSAPI"
    *         security.protocol = "SASL_SSL"
    *       }
    *    }
    * ]
    * }}}
    *
    * Here 'hyperdrive1' is the name of the notification target.
    *
    * @param name         The name of the notification target.
    * @param conf         The configuration of the application.
    * @param overrideConf A configuration containing override keys for the notification target.
    *
    */
  def getByName(name: String,
                conf: Config,
                overrideConf: Option[Config])
               (implicit spark: SparkSession): NotificationTarget = {
    ExternalChannelFactoryReflect.fromConfigByName[NotificationTarget](conf, overrideConf, NOTIFICATION_TARGETS_KEY, name, "notification target")
  }
}
