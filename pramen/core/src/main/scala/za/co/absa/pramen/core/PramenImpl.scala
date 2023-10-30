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

package za.co.absa.pramen.core

import com.typesafe.config.Config
import za.co.absa.pramen.api.{MetadataManager, NotificationBuilder, Pramen}
import za.co.absa.pramen.api.app.PramenFactory
import za.co.absa.pramen.api.common.BuildPropertiesRetriever
import za.co.absa.pramen.core.state.NotificationBuilderImpl
import za.co.absa.pramen.core.utils.BuildPropertyUtils

class PramenImpl extends Pramen {
  private val notificationBuilderImpl = new NotificationBuilderImpl

  private var _workflowConfig: Option[Config] = None

  private var _metadataManager: Option[MetadataManager] = None

  override def buildProperties: BuildPropertiesRetriever = BuildPropertyUtils.instance

  def workflowConfig: Config = _workflowConfig.getOrElse(
    throw new IllegalStateException("Workflow configuration is not available at the context.")
  )

  override def notificationBuilder: NotificationBuilder = notificationBuilderImpl

  override def metadataManager: MetadataManager = _metadataManager.getOrElse(
    throw new IllegalStateException("Metadata manager is not available at the context.")
  )

  private[core] def setWorkflowConfig(config: Config): Unit = _workflowConfig = Option(config)

  private[core] def setMetadataManager(m: MetadataManager): Unit = _metadataManager = Option(m)
}

object PramenImpl extends PramenFactory {
  lazy val instance: Pramen = new PramenImpl
}
