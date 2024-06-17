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

package za.co.absa.pramen.core.state

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.pramen.api.status.RunStatus
import za.co.absa.pramen.core.PramenImpl
import za.co.absa.pramen.core.app.config.HookConfig.SHUTDOWN_HOOK_CLASS_KEY
import za.co.absa.pramen.core.mocks.TaskResultFactory
import za.co.absa.pramen.core.mocks.state.{ShutdownHookFailureMock, ShutdownHookSuccessMock}

class PipelineStateSuite extends AnyWordSpec {
  private implicit val conf: Config = ConfigFactory.parseString(
      """pramen {
        |   pipeline.name = "Test pipeline"
        |   environment.name = "Test"
        |}
        |""".stripMargin)
    .withFallback(ConfigFactory.load())

  "setShutdownHookCanRun" should {
    "set the flag to true" in {
      val stateManager = new PipelineStateImpl()(conf, PramenImpl.instance.notificationBuilder)

      assert(!stateManager.getState().customShutdownHookCanRun)

      stateManager.setShutdownHookCanRun()

      assert(stateManager.getState().customShutdownHookCanRun)
    }
  }

  "setSuccess()" should {
    "set the success flag to true" in {
      val stateManager = getMockPipelineState()

      assert(!stateManager.getState().isFinished)
      assert(!stateManager.getState().exitedNormally)

      stateManager.setSuccess()

      assert(stateManager.getState().isFinished)
      assert(stateManager.getState().exitedNormally)
      assert(stateManager.getState().pipelineInfo.failureException.isEmpty)
    }
  }

  "setFailure()" should {
    "set the failure flag to true" in {
      val stateManager = getMockPipelineState()

      assert(!stateManager.getState().isFinished)
      assert(!stateManager.getState().exitedNormally)

      stateManager.setFailure("test", new RuntimeException("test"))

      assert(stateManager.getState().isFinished)
      assert(!stateManager.getState().exitedNormally)
      assert(stateManager.getState().pipelineInfo.failureException.exists(_.isInstanceOf[RuntimeException]))
    }
  }

  "addTaskCompletion" should {
    "add the task completion statuses" in {
      val stateManager = getMockPipelineState()

      assert(stateManager.getState().taskResults.isEmpty)

      stateManager.addTaskCompletion(Seq(
        TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.Failed(new RuntimeException("test")))
      ))

      assert(stateManager.getState().taskResults.size == 1)
      assert(stateManager.getState().exitCode == 2)
    }
  }

  "alreadyFinished" should {
    "return true is the application is already in process of finishing" in {
      val stateManager = getMockPipelineState()

      stateManager.setSuccess()

      assert(stateManager.alreadyFinished())
    }

  }

  "runCustomShutdownHook" should {
    "do not run if not initialized" in {
      val stateManager = getMockPipelineState(shutdownHookClass = Some("za.co.absa.pramen.core.mocks.state.ShutdownHookSuccessMock"))

      stateManager.runCustomShutdownHook()

      assert(ShutdownHookSuccessMock.ranTimes == 0)
    }

    "run the shutdown hook" in {
      val stateManager = getMockPipelineState(shutdownHookClass = Some("za.co.absa.pramen.core.mocks.state.ShutdownHookSuccessMock"))

      stateManager.setShutdownHookCanRun()
      stateManager.runCustomShutdownHook()

      assert(ShutdownHookSuccessMock.ranTimes > 0)
    }

    "handle fatal errors" in {
      val stateManager = getMockPipelineState(shutdownHookClass = Some("za.co.absa.pramen.core.mocks.state.ShutdownHookFailureMock"))

      stateManager.setShutdownHookCanRun()
      stateManager.runCustomShutdownHook()

      assert(ShutdownHookFailureMock.ranTimes > 0)
      assert(stateManager.getState().pipelineInfo.failureException.exists(_.isInstanceOf[LinkageError]))
    }

    "handle class does not exists errors" in {
      val stateManager = getMockPipelineState(shutdownHookClass = Some("za.co.absa.pramen.core.mocks.state.NotExists"))

      stateManager.setShutdownHookCanRun()
      stateManager.runCustomShutdownHook()

      assert(ShutdownHookFailureMock.ranTimes > 0)
      assert(stateManager.getState().pipelineInfo.failureException.isDefined)
      assert(stateManager.getState().pipelineInfo.failureException.exists(_.isInstanceOf[ClassNotFoundException]))

    }
  }

  def getMockPipelineState(shutdownHookClass: Option[String] = None): PipelineStateImpl = {
    val effectiveConfig = shutdownHookClass match {
      case Some(clazz) =>
        conf.withValue(SHUTDOWN_HOOK_CLASS_KEY, ConfigValueFactory.fromAnyRef(clazz))
      case None =>
        conf
    }
    new PipelineStateImpl()(effectiveConfig, PramenImpl.instance.notificationBuilder) {
      override val log: Logger = LoggerFactory.getLogger("za.co.absa.pramen.core.state.PipelineStateImpl")

      override def sendNotificationEmail(): Unit = {
        // do not actually send the email
      }
    }
  }

}
