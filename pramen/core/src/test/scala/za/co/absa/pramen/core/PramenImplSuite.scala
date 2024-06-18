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

import com.typesafe.config.ConfigFactory
import org.mockito.Mockito.{mock, when}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.NotificationBuilder
import za.co.absa.pramen.api.common.BuildPropertiesRetriever
import za.co.absa.pramen.api.status.RunStatus
import za.co.absa.pramen.core.metadata.MetadataManagerNull
import za.co.absa.pramen.core.mocks.{PipelineStateSnapshotFactory, TaskResultFactory}
import za.co.absa.pramen.core.state.PipelineState

import java.time.LocalDate

class PramenImplSuite extends AnyWordSpec {
  "instance()" should {
    "return the Pramen singleton" in {
      val instance1 = PramenImpl.instance
      val instance2 = PramenImpl.instance

      assert(instance1.isInstanceOf[PramenImpl])
      assert(instance1 == instance2)
    }
  }

  "buildProperties()" should {
    "return the Pramen version retriever instance" in {
      val prop1 = PramenImpl.instance.buildProperties
      val prop2 = PramenImpl.instance.buildProperties

      assert(prop1.isInstanceOf[BuildPropertiesRetriever])
      assert(prop1 == prop2)
    }
  }

  "notificationBuilder()" should {
    "return the notification builder instance" in {
      val builder1 = PramenImpl.instance.notificationBuilder
      val builder2 = PramenImpl.instance.notificationBuilder

      assert(builder1.isInstanceOf[NotificationBuilder])
      assert(builder1 == builder2)
    }
  }

  "metadataManager()" should {
    "return the config if it is available" in {
      val pramen = PramenImpl.instance.asInstanceOf[PramenImpl]
      val config = ConfigFactory.empty()

      pramen.setWorkflowConfig(config)

      val workflowConfig = PramenImpl.instance.workflowConfig

      assert(workflowConfig == config)

      pramen.setWorkflowConfig(null)
    }

    "throw an exception if the config is not available" in {
      val pramen = PramenImpl.instance.asInstanceOf[PramenImpl]

      pramen.setWorkflowConfig(null)

      assertThrows[IllegalStateException] {
        PramenImpl.instance.workflowConfig
      }
    }
  }

  "metadataManager()" should {
    "return the metadata manager if it is available" in {
      val pramen = PramenImpl.instance.asInstanceOf[PramenImpl]

      pramen.setMetadataManager(new MetadataManagerNull(false))

      val manager = PramenImpl.instance.metadataManager

      assert(manager.isInstanceOf[MetadataManagerNull])

      pramen.setMetadataManager(null)
    }

    "throw an exception if metadata manager is not available" in {
      val pramen = PramenImpl.instance.asInstanceOf[PramenImpl]

      pramen.setMetadataManager(null)

      assertThrows[IllegalStateException] {
        PramenImpl.instance.metadataManager
      }
    }
  }

  "pipelineInfo" should {
    "return pipeline info when it is available" in {
      val pramen = PramenImpl.instance.asInstanceOf[PramenImpl]

      val taskResults = Seq(
        TaskResultFactory.getDummyTaskResult(),
        TaskResultFactory.getDummyTaskResult(runInfo = None),
        TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.NotRan)
      )

      val pipelineState = mock(classOf[PipelineState])
      when(pipelineState.getState()).thenReturn(PipelineStateSnapshotFactory.getDummyPipelineStateSnapshot(taskResults = taskResults))

      pramen.setPipelineState(pipelineState)

      val pipelineInfo = PramenImpl.instance.pipelineInfo

      assert(pipelineInfo.pipelineName == "Dummy Pipeline")
      assert(pipelineInfo.environment == "DEV")

      pramen.setPipelineState(null)
    }

    "throw an exception if pipeline state is not available" in {
      val pramen = PramenImpl.instance.asInstanceOf[PramenImpl]

      pramen.setPipelineState(null)

      assertThrows[IllegalStateException] {
        PramenImpl.instance.pipelineInfo
      }
    }
  }

  "getCompletedTasks()" should {
    "return the metadata manager if it is available" in {
      val pramen = PramenImpl.instance.asInstanceOf[PramenImpl]

      val taskResults = Seq(
        TaskResultFactory.getDummyTaskResult(),
        TaskResultFactory.getDummyTaskResult(runInfo = None),
        TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.NotRan)
      )

      val pipelineState = mock(classOf[PipelineState])
      when(pipelineState.getState()).thenReturn(PipelineStateSnapshotFactory.getDummyPipelineStateSnapshot(taskResults = taskResults))

      pramen.setPipelineState(pipelineState)

      val tasks = PramenImpl.instance.getCompletedTasks

      assert(tasks.length == 3)
      assert(tasks.head.runInfo.get.infoDate == LocalDate.of(2022, 2, 18))
      assert(tasks(1).runInfo.isEmpty)

      pramen.setPipelineState(null)
    }

    "throw an exception if the state is not available" in {
      val pramen = PramenImpl.instance.asInstanceOf[PramenImpl]

      pramen.setPipelineState(null)

      assertThrows[IllegalStateException] {
        PramenImpl.instance.getCompletedTasks
      }
    }
  }

  "pipelineState" should {
    "return the current pipeline state if it is available" in {
      val pramen = PramenImpl.instance.asInstanceOf[PramenImpl]

      val taskResults = Seq(
        TaskResultFactory.getDummyTaskResult(),
        TaskResultFactory.getDummyTaskResult(runInfo = None),
        TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.NotRan)
      )

      val pipelineState = mock(classOf[PipelineState])
      when(pipelineState.getState()).thenReturn(PipelineStateSnapshotFactory.getDummyPipelineStateSnapshot(taskResults = taskResults))

      pramen.setPipelineState(pipelineState)

      val state = PramenImpl.instance.pipelineState

      assert(state.taskResults.length == 3)

      pramen.setPipelineState(null)
    }

    "throw an exception if the pipeline state is not available" in {
      val pramen = PramenImpl.instance.asInstanceOf[PramenImpl]

      pramen.setPipelineState(null)

      assertThrows[IllegalStateException] {
        PramenImpl.instance.pipelineState
      }
    }
  }
}
