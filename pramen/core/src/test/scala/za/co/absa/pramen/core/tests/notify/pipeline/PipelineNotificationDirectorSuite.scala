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

package za.co.absa.pramen.core.tests.notify.pipeline

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.notification.{NotificationEntry, TextElement}
import za.co.absa.pramen.api.status.{PipelineNotificationFailure, RunStatus}
import za.co.absa.pramen.core.mocks.notify.PipelineNotificationBuilderSpy
import za.co.absa.pramen.core.mocks.{PipelineNotificationFactory, SchemaDifferenceFactory, TaskResultFactory}
import za.co.absa.pramen.core.notify.pipeline.PipelineNotificationDirector

import java.time.Instant

class PipelineNotificationDirectorSuite extends AnyWordSpec {

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

      val customEntries = List(NotificationEntry.Paragraph(TextElement("1") :: Nil), NotificationEntry.Paragraph(TextElement("1") :: Nil))
      val pipelineNotificationFailures = List(PipelineNotificationFailure("Dummy Reason", new IllegalStateException("Dummy")))

      val notification = PipelineNotificationFactory.getDummyNotification(exception = Some(ex2), warningFlag = true, tasksCompleted =
        List(taskCompleted1, taskCompleted2, taskCompleted3), customEntries = customEntries, pipelineNotificationFailures = pipelineNotificationFailures)

      val builderSpy = new PipelineNotificationBuilderSpy

      PipelineNotificationDirector.build(builderSpy, notification)

      assert(builderSpy.appName == "DummyPipeline")
      assert(builderSpy.environmentName == "DummyEnvironment")
      assert(builderSpy.appStarted == Instant.ofEpochSecond(1234567L))
      assert(builderSpy.appFinished == Instant.ofEpochSecond(1234568L))
      assert(builderSpy.failureException.contains(ex2))
      assert(builderSpy.warningFlag)
      assert(builderSpy.isDryRun.contains(true))
      assert(builderSpy.isUndercover.contains(false))
      assert(builderSpy.minRps == 1000)
      assert(builderSpy.goodRps == 2000)
      assert(builderSpy.addCompletedTaskCalled == 3)
      assert(builderSpy.addCustomEntriesCalled == 1)
      assert(builderSpy.addPipelineNotificationFailure == 1)
    }
  }

}
