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
import za.co.absa.pramen.api.PipelineNotificationTarget
import za.co.absa.pramen.core.utils.ClassLoaderUtils

import scala.collection.JavaConverters._

object PipelineNotificationTargetFactory {
  val PIPELINE_NOTIFICATION_FACTORIES_KEY = "pramen.pipeline.notification.targets"

  def fromConfig(conf: Config): Seq[PipelineNotificationTarget] = {
    if (conf.hasPath(PIPELINE_NOTIFICATION_FACTORIES_KEY)) {
      val factories = conf.getStringList(PIPELINE_NOTIFICATION_FACTORIES_KEY).asScala
      factories.map(clazz => createPipelineNotificationTarget(clazz, conf)).toSeq
    } else {
      Seq.empty
    }
  }

  private[core] def createPipelineNotificationTarget(clazz: String, appConfig: Config): PipelineNotificationTarget = {
    ClassLoaderUtils.loadConfigurableClass[PipelineNotificationTarget](clazz, appConfig)
  }

}
