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

package za.co.absa.pramen.api

import com.typesafe.config.{Config, ConfigFactory}
import za.co.absa.pramen.api.app.PramenFactory
import za.co.absa.pramen.api.common.{BuildPropertiesRetriever, FactoryLoader}
import za.co.absa.pramen.api.lock.TokenLockFactory
import za.co.absa.pramen.api.status.{PipelineStateSnapshot, TaskResult}

import scala.util.Try

/**
  * Pramen provides additional features via a client that you can access like this:
  * {{{
  *   val pramen = Pramen.instance
  * }}}
  *
  * 'pramen-core' should be in the runtime classpath if you want to use this.
  * For tests that do not use 'pramen-core' you can create a dummy implementation in 'za.co.absa.pramen.core.state'.
  */
trait Pramen {
  /** Gets an object that contains Pramen runtime version information. */
  def buildProperties: BuildPropertiesRetriever

  /** This gives access to the current workflow configuration. */
  def workflowConfig: Config

  /** General information about the running pipeline. */
  def pipelineInfo: PipelineInfo

  /** The current pipeline state. */
  def pipelineState: PipelineStateSnapshot

  /** Gets the notification builder that you can use to add custom information to email notifications. */
  def notificationBuilder: NotificationBuilder

  /**
    * Returns metadata manager that can be used to get set metastore tables metadata.
    * This is method is used from custom sources. In transformers and sinks you can use
    * {{{metastore.metadataManager}}}
    */
  def metadataManager: MetadataManager

  /**
    * Returns the list of tasks completed at the current moment of time.
    */
  def getCompletedTasks: Seq[TaskResult]

  /**
    * Sets the warning flag, which means that the pipeline will be reported as succeeded with warnings.
    *
    * This can be set from any operation, but cannot be unset.
    */
  def setWarningFlag(): Unit

  /**
    * Provides a factory for creating and managing token-based locks, which can be used for
    * tasks like ensuring only one instance of a process is running or managing resource access.
    *
    * @return An instance of the TokenLockFactory, which allows for token-based locking functionality.
    */
  def tokenLockFactory: TokenLockFactory
}

object Pramen {
  val PRAMEN_NOTIFICATION_BUILDER_FACTORY_CLASS = "za.co.absa.pramen.core.PramenImpl"

  /**
    * Returns the instance of Pramen client.
    * Projects can depend only on `pramen-api`, but the implementation of this interface is in `pramen-core`.
    * IllegalArgumentException will be thrown if `pramen-core` is not in the runtime classpath or if the implementation
    * class cannot be loaded for some reason.
    */
  lazy val instance: Pramen = FactoryLoader.loadSingletonFactoryOfType[PramenFactory](PRAMEN_NOTIFICATION_BUILDER_FACTORY_CLASS).instance

  /**
    * Returns the configuration of the currently running workflow.
    *
    * If no workflow is running at the moment, returns the default TypeSafe config
    * which combines reference.conf and application.conf.
    */
  def getConfig: Config = {
    Try {
      Pramen.instance.workflowConfig
    }.toOption.getOrElse(ConfigFactory.load())
  }
}
