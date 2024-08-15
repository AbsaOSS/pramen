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
import za.co.absa.pramen.api.app.PramenFactory
import za.co.absa.pramen.api.common.BuildPropertiesRetriever
import za.co.absa.pramen.api.status.{PipelineStateSnapshot, TaskResult}
import za.co.absa.pramen.api.{MetadataManager, NotificationBuilder, PipelineInfo, Pramen}
import za.co.absa.pramen.core.state.{NotificationBuilderImpl, PipelineState}
import za.co.absa.pramen.core.utils.BuildPropertyUtils

class PramenImpl extends Pramen {
  private val notificationBuilderImpl = new NotificationBuilderImpl

  private var _workflowConfig: Option[Config] = None

  private var _metadataManager: Option[MetadataManager] = None

  private var _pipelineState: Option[PipelineState] = None

  override def buildProperties: BuildPropertiesRetriever = BuildPropertyUtils.instance

  def workflowConfig: Config = _workflowConfig.getOrElse(
    throw new IllegalStateException("Workflow configuration is not available at the context.")
  )

  override def pipelineInfo: PipelineInfo = {
    val pipelineState = _pipelineState.getOrElse(
      throw new IllegalStateException("Pipeline state is not available at the context.")
    )

    pipelineState.getState().pipelineInfo
  }

  override def pipelineState: PipelineStateSnapshot = {
    val pipelineState = _pipelineState.getOrElse(
      throw new IllegalStateException("Pipeline state is not available at the context.")
    )

    pipelineState.getState()
  }

  override def notificationBuilder: NotificationBuilder = notificationBuilderImpl

  override def metadataManager: MetadataManager = _metadataManager.getOrElse(
    throw new IllegalStateException("Metadata manager is not available at the context.")
  )

  override def getCompletedTasks: Seq[TaskResult] = {
    val pipelineState = _pipelineState.getOrElse(
      throw new IllegalStateException("Pipeline state is not available at the context.")
    )

    val state = pipelineState.getState()

    state.taskResults
  }

  override def setWarningFlag(): Unit = {
    val pipelineState = _pipelineState.getOrElse(
      throw new IllegalStateException("Pipeline state is not available at the context.")
    )

    pipelineState.setWarningFlag()
  }

  private[core] def setWorkflowConfig(config: Config): Unit = synchronized {
    _workflowConfig = Option(config)
  }

  private[core] def setMetadataManager(m: MetadataManager): Unit = synchronized {
    _metadataManager = Option(m)
  }

  private[core] def setPipelineState(s: PipelineState): Unit = synchronized {
    _pipelineState = Option(s)
  }

  private[core] def reset(): Unit = synchronized {
    notificationBuilderImpl.reset()
    _workflowConfig = None
    _metadataManager = None
  }
}

object PramenImpl extends PramenFactory {
  val instance: Pramen = new PramenImpl

  private[core] def reset(): Unit = this.synchronized {
    instance.asInstanceOf[PramenImpl].reset()
  }
}
