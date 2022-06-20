/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.tests.notify

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.fixtures.TextComparisonFixture
import za.co.absa.pramen.framework.mocks.{SchemaDifferenceFactory, TaskCompletedFactory}
import za.co.absa.pramen.framework.notify.NotificationBuilderHtml
import za.co.absa.pramen.framework.utils.ResourceUtils

import java.time.Instant

class NotificationBuilderHtmlSuite extends WordSpec with TextComparisonFixture {
  "constructor" should {
    "be able to initialize the builder with the default timezone" in {
      implicit val conf: Config = ConfigFactory.parseString(
        """pramen {
          |  application.version = 1.0.0
          |}
          |""".stripMargin)

      val builder = new NotificationBuilderHtml()

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
      val expected = ResourceUtils.getResourceString("/test/notify/test_email_body_default.txt")

      val builder = getBuilder

      val actual = builder.renderBody()

      compareText(actual, expected)
    }

    "render a notification body with completed tasks and schema changes" in {
      val expected = ResourceUtils.getResourceString("/test/notify/test_email_body_complex.txt")

      val builder = getBuilder


      builder.addDryRun(true)
      builder.addUndercover(true)

      builder.addRpsMetrics(1000, 2000)
      builder.addCompletedTask(TaskCompletedFactory.getTackCompleted())
      builder.addSchemaDifferences(SchemaDifferenceFactory.getDummySchemaDifference())

      val actual = builder.renderBody()

      compareText(actual, expected)
    }

    "render a notification body with an exception" in {
      val builder = getBuilder

      builder.addFailureException(new IllegalArgumentException("MyTest exception"))

      val actual = builder.renderBody()

      // Can't test the full body since stack trace depends on the runner of the unit test
      assert(actual.contains("<pre>java.lang.IllegalArgumentException: MyTest exception"))
    }
  }


  def getBuilder: NotificationBuilderHtml = {
    implicit val conf: Config = ConfigFactory.parseString(
      """pramen {
        |  application.version = 1.0.0
        |  timezone = "Africa/Johannesburg"
        |}
        |""".stripMargin)

    val builder = new NotificationBuilderHtml()

    builder.addAppName("MyApp")
    builder.addEnvironmentName("MyEnv")
    builder.addAppDuration(Instant.ofEpochSecond(1639733518L), Instant.ofEpochSecond(1639734629L))

    builder
  }

}
