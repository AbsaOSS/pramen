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

package za.co.absa.pramen.framework.tests.notify.pipeline

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.mocks.notify.PipelineNotificationBuilderSpy
import za.co.absa.pramen.framework.mocks.{PipelineNotificationFactory, SchemaDifferenceFactory, TaskResultFactory}
import za.co.absa.pramen.framework.notify.pipeline.PipelineNotificationDirector
import za.co.absa.pramen.framework.runner.task.RunStatus

import java.time.Instant

class PipelineNotificationDirectorSuite extends WordSpec {

  "build()" should {
    "execute build steps for a notification" in {
      implicit val conf: Config = ConfigFactory.parseString(
        """pramen {
          |  warn.throughput.rps = 1000
          |  good.throughput.rps = 2000
          |
          |  dry.run = true
          |  undercover = false
          |}
          |""".stripMargin)

      val ex1 = new IllegalArgumentException("Dummy1 exception")
      val ex2 = new IllegalArgumentException("Dummy2 exception")

      val schemaDifferences = List(SchemaDifferenceFactory.getDummySchemaDifference(), SchemaDifferenceFactory.getDummySchemaDifference())
      val taskCompleted1 = TaskResultFactory.getDummyTaskResult()
      val taskCompleted2 = TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.Failed(ex1))
      val taskCompleted3 = TaskResultFactory.getDummyTaskResult(schemaDifferences = schemaDifferences)

      val notification = PipelineNotificationFactory.getDummyNotification(exception = Some(ex2), tasksCompleted = List(taskCompleted1, taskCompleted2, taskCompleted3))

      val builderSpy = new PipelineNotificationBuilderSpy

      PipelineNotificationDirector.build(builderSpy, notification)

      assert(builderSpy.appName == "DummyPipeline")
      assert(builderSpy.environmentName == "DummyEnvironment")
      assert(builderSpy.appStarted == Instant.ofEpochSecond(1234567L))
      assert(builderSpy.appFinished == Instant.ofEpochSecond(1234568L))
      assert(builderSpy.failureException.contains(ex2))
      assert(builderSpy.isDryRun.contains(true))
      assert(builderSpy.isUndercover.contains(false))
      assert(builderSpy.minRps == 1000)
      assert(builderSpy.goodRps == 2000)
      assert(builderSpy.addCompletedTaskCalled == 3)
    }
  }

}
