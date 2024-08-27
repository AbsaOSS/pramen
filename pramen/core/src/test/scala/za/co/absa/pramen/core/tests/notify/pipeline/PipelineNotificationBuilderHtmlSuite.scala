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
import za.co.absa.pramen.api.notification.NotificationEntry.Paragraph
import za.co.absa.pramen.api.notification._
import za.co.absa.pramen.api.status.{DependencyWarning, NotificationFailure, PipelineNotificationFailure, RunStatus, TaskRunReason}
import za.co.absa.pramen.core.exceptions.{CmdFailedException, ProcessFailedException}
import za.co.absa.pramen.core.fixtures.TextComparisonFixture
import za.co.absa.pramen.core.mocks.{RunStatusFactory, SchemaDifferenceFactory, TaskResultFactory, TestPrototypes}
import za.co.absa.pramen.core.notify.message.{MessageBuilderHtml, ParagraphBuilder}
import za.co.absa.pramen.core.notify.pipeline.PipelineNotificationBuilderHtml
import za.co.absa.pramen.core.notify.pipeline.PipelineNotificationBuilderHtml.{NOTIFICATION_EXCEPTION_MAX_LENGTH_KEY, NOTIFICATION_REASON_MAX_LENGTH_KEY}
import za.co.absa.pramen.core.utils.ResourceUtils

import java.time.{Instant, LocalDate}

class PipelineNotificationBuilderHtmlSuite extends AnyWordSpec with TextComparisonFixture {
  private val megabyte = 1024L * 1024L
  private val emptyConfig = ConfigFactory.empty()

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
      val builder = getBuilder()

      builder.addAppName("MyApp")

      assert(builder.renderSubject().startsWith("Notification of SUCCESS for MyApp at"))
    }

    "render a dry run subject" in {
      val builder = getBuilder()

      builder.addAppName("MyNewApp")
      builder.addDryRun(true)

      assert(builder.renderSubject().startsWith("(DRY RUN) Notification of SUCCESS for MyNewApp at"))
    }

    "render failure" in {
      val builder = getBuilder()

      builder.addAppName("MyNewApp")
      builder.addFailureException(new RuntimeException("Test exception"))

      assert(builder.renderSubject().startsWith("Notification of FAILURE for MyNewApp"))
    }

    "render warning" in {
      val builder = getBuilder()

      builder.addAppName("MyNewApp")
      builder.addCompletedTask(TaskResultFactory.getDummyTaskResult(runStatus = TestPrototypes.runStatusWarning))

      assert(builder.renderSubject().startsWith("Notification of WARNING for MyNewApp"))
    }

    "render partial failure" in {
      val builder = getBuilder()

      builder.addAppName("MyNewApp")
      builder.addCompletedTask(TaskResultFactory.getDummyTaskResult(runStatus = TestPrototypes.runStatusWarning))
      builder.addCompletedTask(TaskResultFactory.getDummyTaskResult(runStatus = TestPrototypes.runStatusFailure))

      assert(builder.renderSubject().startsWith("Notification of PARTIAL SUCCESS for MyNewApp at"))
    }
  }

  "renderBody()" should {
    "render a default notification body" in {
      val expected = ResourceUtils.getResourceString("/test/notify/test_pipeline_email_body_default.txt")

      val builder = getBuilder()

      val actual = builder.renderBody()
        .split("\n")
        .dropRight(5) // dropping lines that contain build version and timestamp
        .mkString("\n")

      compareText(actual, expected)
    }

    "render a notification body with completed tasks and schema changes and custom entries" in {
      val expected = ResourceUtils.getResourceString("/test/notify/test_pipeline_email_body_complex.txt")

      val builder = getBuilder()

      builder.addDryRun(true)
      builder.addUndercover(true)

      builder.addRpsMetrics(1000, 2000)
      builder.addCompletedTask(TaskResultFactory.getDummyTaskResult(
        runStatus = TestPrototypes.runStatusWarning,
        schemaDifferences = SchemaDifferenceFactory.getDummySchemaDifference() :: Nil)
      )

      builder.addCustomEntries(Seq(
        NotificationEntry.Paragraph(TextElement("Custom text 1") :: TextElement("Custom text 2", Style.Error) :: Nil),
        NotificationEntry.Table(
          TableHeader(TextElement("Header 1"), Align.Right) :: TableHeader(TextElement("Header 2"), Align.Center) :: Nil, Seq(
            TextElement("Cell 1, 1") :: TextElement("Cell 1, 2") :: Nil,
            TextElement("Cell 2, 1") :: TextElement("Cell 2, 2") :: Nil,
            TextElement("Cell 3, 1") :: TextElement("Cell 3, 2") :: Nil
          )),
        NotificationEntry.UnorderedList(Seq(
          Paragraph(ParagraphBuilder().withText("Item 1").paragraph),
          Paragraph(ParagraphBuilder().withText("Item 2").paragraph)
        )),
        NotificationEntry.OrderedList(Seq(
          Paragraph(ParagraphBuilder().withText("Ordered 1").paragraph),
          Paragraph(ParagraphBuilder().withText("Ordered 2").paragraph)
        )),
        NotificationEntry.AttachedFile("file1.txt", "This is a test file".getBytes),
        NotificationEntry.Html("<p><b>This is a test HTML block</b></p>")
      ))

      builder.addSignature(TextElement("Test signature"))

      val actual = builder.renderBody()

      compareText(actual, expected)
    }

    "render a notification body with an exception" in {
      val builder = getBuilder()

      val nestedCause1 = new RuntimeException("Cause 1")
      val nestedCause2 = new RuntimeException("Cause 2", nestedCause1)

      builder.addFailureException(new IllegalArgumentException("MyTest exception", nestedCause2))

      builder.addCompletedTask(TaskResultFactory.getDummyTaskResult(
        runStatus = TestPrototypes.runStatusWarning,
        schemaDifferences = SchemaDifferenceFactory.getDummySchemaDifference() :: Nil,
        notificationTargetErrors = Seq(NotificationFailure("table1", "my_tagret", LocalDate.parse("2020-02-18"), new RuntimeException("Target 1 exception"))))
      )

      val actual = builder.renderBody()

      // Can't test the full body since stack trace depends on the runner of the unit test
      assert(actual.contains("<pre>java.lang.IllegalArgumentException: MyTest exception"))
      assert(actual.contains("Cause 1"))
      assert(actual.contains("Cause 2"))
      assert(actual.contains("Failed to send notifications to the following targets"))
    }
  }

  "renderJobException" should {
    "render a job exception" in {
      val notificationBuilder = getBuilder()
      val messageBuilder = new MessageBuilderHtml(emptyConfig)

      val taskResult = TaskResultFactory.getDummyTaskResult(runStatus = TestPrototypes.runStatusFailure)

      val actual = notificationBuilder.renderJobException(messageBuilder, taskResult, new RuntimeException("Job Exception"))
        .renderBody

      assert(actual.contains("""<span class="tderr">DummyJob</span><span class="tdred"> outputting to </span><span class="tderr">table_out</span><span class="tdred"> at </span><span class="tderr">2022-02-18</span><span class="tdred"> has failed with an exception:"""))
      assert(actual.contains("""</span><span class="tderr">Job Exception</span>"""))
      assert(actual.contains("""<pre>java.lang.RuntimeException: Job Exception"""))
    }

    "render a job exception with a limit on size" in {
      val conf = ConfigFactory.parseString(
        s"""$NOTIFICATION_REASON_MAX_LENGTH_KEY = 10
           |$NOTIFICATION_EXCEPTION_MAX_LENGTH_KEY = 15
           |""".stripMargin
      )
      val notificationBuilder = getBuilder(conf)
      val messageBuilder = new MessageBuilderHtml(emptyConfig)

      val taskResult = TaskResultFactory.getDummyTaskResult(runStatus = TestPrototypes.runStatusFailure)

      val actual = notificationBuilder.renderJobException(messageBuilder, taskResult, new RuntimeException("Job Exception"))
        .renderBody

      assert(actual.contains("""<span class="tderr">DummyJob</span><span class="tdred"> outputting to </span><span class="tderr">table_out</span><span class="tdred"> at </span><span class="tderr">2022-02-18</span><span class="tdred"> has failed with an exception:"""))
      assert(actual.contains("""<span class="tderr">Job Except...</span>"""))
      assert(actual.contains("""<pre>java.lang.Runti...</pre>"""))
    }
  }

  "renderException" should {
    "render runtime exceptions" in {
      val notificationBuilder = getBuilder()
      val messageBuilder = new MessageBuilderHtml(emptyConfig)

      val ex = new RuntimeException("Text exception")

      val actual = notificationBuilder.renderException(messageBuilder, ex)
        .renderBody

      assert(actual.contains("""<pre>java.lang.RuntimeException: Text exception"""))
      assert(actual.contains("""za.co.absa.pramen.core.tests.notify.pipeline.PipelineNotificationBuilderHtmlSuite"""))
    }

    "render runtime exceptions with a limit on its size" in {
      val conf = ConfigFactory.parseString(
        s"""$NOTIFICATION_EXCEPTION_MAX_LENGTH_KEY = 10
          |""".stripMargin
      )
      val notificationBuilder = getBuilder(conf)
      val messageBuilder = new MessageBuilderHtml(conf)

      val ex = new RuntimeException("Text exception")

      val actual = notificationBuilder.renderException(messageBuilder, ex)
        .renderBody

      assert(actual.contains("""<pre>java.lang....</pre>"""))
    }

    "render command line exceptions with logs" in {
      val notificationBuilder = getBuilder()
      val messageBuilder = new MessageBuilderHtml(emptyConfig)

      val ex = CmdFailedException("Command line failed", Array("Log line 1", "Log line 2"))

      val actual = notificationBuilder.renderException(messageBuilder, ex)
        .renderBody

      assert(actual.contains(
        """<pre>Command line failed
          |Last log lines:
          |Log line 1
          |Log line 2
          |</pre>""".stripMargin.replaceAll("\\r\\n", "\\n")
      ))
    }

    "render command line exceptions without logs" in {
      val notificationBuilder = getBuilder()
      val messageBuilder = new MessageBuilderHtml(emptyConfig)

      val ex = CmdFailedException("Command line failed", Array.empty)

      val actual = notificationBuilder.renderException(messageBuilder, ex)
        .renderBody

      assert(actual.contains("""<pre>Command line failed</pre>"""))
    }

    "render process execution exceptions" in {
      val notificationBuilder = getBuilder()
      val messageBuilder = new MessageBuilderHtml(emptyConfig)

      val ex = ProcessFailedException("Command line failed", Array("stdout line 1", "stdout line 2"), Array("stderr line 1"))

      val actual = notificationBuilder.renderException(messageBuilder, ex)
        .renderBody

      assert(actual.contains(
        """<pre>Command line failed
          |Last <b>stdout</b> lines:
          |stdout line 1
          |stdout line 2
          |
          |Last <b>stderr</b> lines:
          |stderr line 1
          |</pre>""".stripMargin.replaceAll("\\r\\n", "\\n")
      ))
    }
  }

  "renderPipelineNotificationFailures" should {
    "render pipeline failure as an exception" in {
      val notificationBuilder = getBuilder()
      val messageBuilder = new MessageBuilderHtml(emptyConfig)

      val ex = new RuntimeException("Test exception")
      notificationBuilder.addPipelineNotificationFailure(PipelineNotificationFailure("com.example.MyNotification", ex))

      val actual = notificationBuilder.renderPipelineNotificationFailures(messageBuilder)
        .renderBody

      assert(actual.contains(
        """<body><p><span class="tdred">Failed to send pipeline notification via 'com.example.MyNotification': </span></p>""".stripMargin.replaceAll("\\r\\n", "\\n")
      ))

      assert(actual.contains(
        """<pre>java.lang.RuntimeException: Test exception""".stripMargin.replaceAll("\\r\\n", "\\n")
      ))
    }
  }

  "getStatus" should {
    "work for succeeded" in {
      val builder = getBuilder()

      val actual = builder.getStatus(TaskResultFactory.getDummyTaskResult(runStatus = TestPrototypes.runStatusSuccess))

      assert(actual == TextElement("Success", Style.Success))
    }

    "work for insufficient data" in {
      val builder = getBuilder()

      val actual = builder.getStatus(TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.InsufficientData(100, 200, None)))

      assert(actual == TextElement("Insufficient data", Style.Exception))
    }

    "work for no data" in {
      val builder = getBuilder()

      val actual = builder.getStatus(TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.NoData(true)))

      assert(actual == TextElement("No Data", Style.Exception))
    }

    "work for skipped" in {
      val builder = getBuilder()

      val actual = builder.getStatus(TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.Skipped("dummy")))

      assert(actual == TextElement("Skipped", Style.Success))
    }

    "work for skipped with warnings" in {
      val builder = getBuilder()

      val actual = builder.getStatus(TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.Skipped("dummy", isWarning = true)))

      assert(actual == TextElement("Skipped", Style.Warning))
    }

    "work for not ran" in {
      val builder = getBuilder()

      val actual = builder.getStatus(TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.NotRan))

      assert(actual == TextElement("Skipped", Style.Warning))
    }

    "work for validation failure" in {
      val builder = getBuilder()

      val actual = builder.getStatus(TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.ValidationFailed(new RuntimeException("dummy"))))

      assert(actual == TextElement("Validation failed", Style.Warning))
    }

    "work for failed dependencies" in {
      val builder = getBuilder()

      val actual = builder.getStatus(TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.FailedDependencies(isFailure = true, Seq.empty)))

      assert(actual == TextElement("Skipped", Style.Warning))
    }

    "work for failed jobs" in {
      val builder = getBuilder()

      val actual = builder.getStatus(TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.Failed(new RuntimeException("dummy"))))

      assert(actual == TextElement("Failed", Style.Exception))
    }
  }

  "getThroughputRps" should {
    "work for a failed tasks" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummyFailure()
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus)

      val actual = builder.getThroughputRps(task)

      assert(actual.text.isEmpty)
    }

    "work for a task without a run info" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(None, 1000000, reason = TaskRunReason.New)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus, runInfo = None)

      val actual = builder.getThroughputRps(task)

      assert(actual.text.isEmpty)
    }

    "work for a normal successful task" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(None, 1000000, reason = TaskRunReason.New)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus)

      val actual = builder.getThroughputRps(task)

      assert(actual.text == "225 r/s")
    }

    "work for a raw file task" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(None, 1000 * megabyte, reason = TaskRunReason.New)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus, isRawFilesJob = true)

      val actual = builder.getThroughputRps(task)

      assert(actual.text == "230 KiB/s")
    }
  }

  "getBytesPerSecondsText" should {
    "return an empty string for files smaller than the minimum size" in {
      val builder = getBuilder()

      val actual = builder.getBytesPerSecondsText(1000, 10)

      assert(actual.text.isEmpty)
    }

    "return the throughput for usual inputs" in {
      val builder = getBuilder()

      val actual = builder.getBytesPerSecondsText(100L * 1024L * 1024L, 10)

      assert(actual.text == "10 MiB/s")
    }
  }

  "getRecordCountText" should {
    "work for a failure" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummyFailure()
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus)

      val actual = builder.getRecordCountText(task)

      assert(actual.isEmpty)
    }

    "work for success file based job" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(Some(100), 100, reason = TaskRunReason.Update)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus, isRawFilesJob = true)

      val actual = builder.getRecordCountText(task)

      assert(actual == "-")
    }

    "work for success new" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(None, 100, reason = TaskRunReason.New)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus)

      val actual = builder.getRecordCountText(task)

      assert(actual == "100")
    }

    "work for success unchanged" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(Some(100), 100, reason = TaskRunReason.Update)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus)

      val actual = builder.getRecordCountText(task)

      assert(actual == "100")
    }

    "work for success increased" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(Some(100), 110, reason = TaskRunReason.Update)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus)

      val actual = builder.getRecordCountText(task)

      assert(actual == "110 (+10)")
    }

    "work for success decreased" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(Some(100), 90, reason = TaskRunReason.Update)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus)

      val actual = builder.getRecordCountText(task)

      assert(actual == "90 (-10)")
    }

    "work for insufficient data" in {
      val builder = getBuilder()

      val task = TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.InsufficientData(90, 96, Some(100)))

      val actual = builder.getRecordCountText(task)

      assert(actual == "90 (-10)")
    }
  }

  "getSizeText" should {
    "work for a failure" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummyFailure()
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus)

      val actual = builder.getSizeText(task)

      assert(actual.isEmpty)
    }

    "work for success new" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(None, 100 * megabyte, reason = TaskRunReason.New)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus, isRawFilesJob = true)

      val actual = builder.getSizeText(task)

      assert(actual == "100 MiB")
    }

    "work for success unchanged" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(Some(100 * megabyte), 100 * megabyte, reason = TaskRunReason.Update)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus, isRawFilesJob = true)

      val actual = builder.getSizeText(task)

      assert(actual == "100 MiB")
    }

    "work for success increased" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(Some(100 * megabyte), 110 * megabyte, reason = TaskRunReason.Update)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus, isRawFilesJob = true)

      val actual = builder.getSizeText(task)

      assert(actual == "110 MiB (+10 MiB)")
    }

    "work for success decreased" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess(Some(100 * megabyte), 90 * megabyte, reason = TaskRunReason.Update)
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus, isRawFilesJob = true)

      val actual = builder.getSizeText(task)

      assert(actual == "90 MiB (-10 MiB)")
    }

    "work for insufficient data" in {
      val builder = getBuilder()

      val task = TaskResultFactory.getDummyTaskResult(runStatus = RunStatus.InsufficientData(90 * megabyte, 96 * megabyte, Some(100 * megabyte)), isRawFilesJob = true)

      val actual = builder.getSizeText(task)

      assert(actual == "90 MiB (-10 MiB)")
    }
  }

  "getFailureReason" should {
    "get the underlying reason" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummyFailure()
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus)

      val actual = builder.getFailureReason(task)

      assert(actual == "Dummy failure")
    }

    "get an empty reason if dependency warning list is empty" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess()
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus)

      val actual = builder.getFailureReason(task)

      assert(actual == "")
    }

    "get an non-empty reason if dependency warning list is non-empty" in {
      val builder = getBuilder()

      val runStatus = RunStatusFactory.getDummySuccess()
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus,
        dependencyWarnings = Seq(DependencyWarning("t1"),DependencyWarning("t2")))

      val actual = builder.getFailureReason(task)

      assert(actual == "Optional dependencies failed for: t1, t2")
    }

    "get an non-empty reason if the limit on reason size is specified" in {
      val conf = ConfigFactory.parseString(
        s"""$NOTIFICATION_REASON_MAX_LENGTH_KEY = 15
           |""".stripMargin
      )

      val builder = getBuilder(conf)

      val runStatus = RunStatusFactory.getDummySuccess()
      val task = TaskResultFactory.getDummyTaskResult(runStatus = runStatus,
        dependencyWarnings = Seq(DependencyWarning("t1"),DependencyWarning("t2")))

      val actual = builder.getFailureReason(task)

      assert(actual == "Optional depend...")
    }
  }

  def getBuilder(conf: Config = emptyConfig): PipelineNotificationBuilderHtml  = {
    implicit val implicitConfig: Config =
      conf.withFallback(
        ConfigFactory.parseString(
          """pramen {
            |  application.version = 1.0.0
            |  timezone = "Africa/Johannesburg"
            |}
            |""".stripMargin)
      )

    val builder = new PipelineNotificationBuilderHtml

    builder.addAppName("MyApp")
    builder.addEnvironmentName("MyEnv")
    builder.addAppDuration(Instant.ofEpochSecond(1647872996L), Instant.ofEpochSecond(1647882996L))

    builder
  }

}
