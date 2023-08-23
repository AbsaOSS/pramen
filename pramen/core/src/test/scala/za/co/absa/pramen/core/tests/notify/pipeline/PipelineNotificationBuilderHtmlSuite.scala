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
import za.co.absa.pramen.api.notification.{Align, NotificationEntry, Style, TableHeader, TextElement}
import za.co.absa.pramen.core.fixtures.TextComparisonFixture
import za.co.absa.pramen.core.mocks.{SchemaDifferenceFactory, TaskResultFactory}
import za.co.absa.pramen.core.notify.pipeline.PipelineNotificationBuilderHtml
import za.co.absa.pramen.core.utils.ResourceUtils

import java.time.Instant

class PipelineNotificationBuilderHtmlSuite extends AnyWordSpec with TextComparisonFixture {
  "constructor" should {
    "be able to initialize the builder with the default timezone" in {
      implicit val conf: Config = ConfigFactory.parseString(
        """pramen {
          |  application.version = 1.0.0
          |}
          |""".stripMargin)

      val builder = new PipelineNotificationBuilderHtml()

      assert(builder != null)
    }
  }

  "renderSubject()" should {
    "render normal subject" in {
      val builder = getBuilder

      builder.addAppName("MyApp")

      assert(builder.renderSubject().startsWith("Notification for MyApp at"))
    }

    "render a dry run subject" in {
      val builder = getBuilder

      builder.addAppName("MyNewApp")
      builder.addDryRun(true)

      assert(builder.renderSubject().startsWith("(DRY RUN) Notification for MyNewApp at"))
    }
  }

  "renderBody()" should {
    "render a default notification body" in {
      val expected = ResourceUtils.getResourceString("/test/notify/test_pipeline_email_body_default.txt")

      val builder = getBuilder

      val actual = builder.renderBody()
        .split("\n")
        .dropRight(5) // dropping lines that contain build version and timestamp
        .mkString("\n")

      compareText(actual, expected)
    }

    "render a notification body with completed tasks and schema changes and custom entries" in {
      val expected = ResourceUtils.getResourceString("/test/notify/test_pipeline_email_body_complex.txt")

      val builder = getBuilder

      builder.addDryRun(true)
      builder.addUndercover(true)

      builder.addRpsMetrics(1000, 2000)
      builder.addCompletedTask(TaskResultFactory.getDummyTaskResult(schemaDifferences = SchemaDifferenceFactory.getDummySchemaDifference() :: Nil))

      builder.addCustomEntries(Seq(
        NotificationEntry.Paragraph(TextElement("Custom text 1") :: TextElement("Custom text 2", Style.Error) :: Nil),
        NotificationEntry.Table(
          TableHeader(TextElement("Header 1"), Align.Right) :: TableHeader(TextElement("Header 2"), Align.Center) :: Nil, Seq(
          TextElement("Cell 1, 1") :: TextElement("Cell 1, 2") :: Nil,
          TextElement("Cell 2, 1") :: TextElement("Cell 2, 2") :: Nil,
          TextElement("Cell 3, 1") :: TextElement("Cell 3, 2") :: Nil
        )))
      )

      val actual = builder.renderBody()
        .split("\n")
        .dropRight(5) // dropping lines that contain build version and timestamp
        .mkString("\n")

      compareText(actual, expected)
    }

    "render a notification body with an exception" in {
      val builder = getBuilder

      val nestedCause1 = new RuntimeException("Cause 1")
      val nestedCause2 = new RuntimeException("Cause 2", nestedCause1)

      builder.addFailureException(new IllegalArgumentException("MyTest exception", nestedCause2))

      val actual = builder.renderBody()

      // Can't test the full body since stack trace depends on the runner of the unit test
      assert(actual.contains("<pre>java.lang.IllegalArgumentException: MyTest exception"))
      assert(actual.contains("Cause 1"))
      assert(actual.contains("Cause 2"))
    }
  }


  def getBuilder: PipelineNotificationBuilderHtml = {
    implicit val conf: Config = ConfigFactory.parseString(
      """pramen {
        |  application.version = 1.0.0
        |  timezone = "Africa/Johannesburg"
        |}
        |""".stripMargin)

    val builder = new PipelineNotificationBuilderHtml()

    builder.addAppName("MyApp")
    builder.addEnvironmentName("MyEnv")
    builder.addAppDuration(Instant.ofEpochSecond(1647872996L), Instant.ofEpochSecond(1647882996L))

    builder
  }

}
