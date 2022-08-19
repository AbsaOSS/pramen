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

package za.co.absa.pramen.core.notify.pipeline

import com.typesafe.config.Config
import za.co.absa.pramen.core.config.Keys.TIMEZONE
import za.co.absa.pramen.core.exceptions.{CmdFailedException, ProcessFailedException}
import za.co.absa.pramen.core.notify.message._
import za.co.absa.pramen.core.notify.pipeline.PipelineNotificationBuilderHtml.MIN_RPS_JOB_DURATION_SECONDS
import za.co.absa.pramen.core.notify.{FieldChange, SchemaDifference}
import za.co.absa.pramen.core.pipeline.{DependencyFailure, TaskRunReason}
import za.co.absa.pramen.core.runner.task.RunStatus._
import za.co.absa.pramen.core.runner.task.TaskResult
import za.co.absa.pramen.core.utils.{BuildProperties, ConfigUtils, StringUtils, TimeUtils}

import java.time._
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.compat.Platform.EOL

object PipelineNotificationBuilderHtml {
  val MIN_RPS_JOB_DURATION_SECONDS = 60
}

class PipelineNotificationBuilderHtml(implicit conf: Config) extends PipelineNotificationBuilder {

  private val timestampFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm Z")

  private val zoneId = getZoneId

  private var minRps = 0
  private var goodRps = Int.MaxValue

  var appException: Option[Throwable] = None
  var appName: String = "Unspecified Job"
  var envName: String = "Unspecified Environment"
  var appStarted: Instant = Instant.now()
  var appFinished: Instant = Instant.now()
  var isDryRun = false
  var isUndercover = false

  val completedTasks = new ListBuffer[TaskResult]

  override def addFailureException(ex: Throwable): Unit = {
    appException = Option(ex)
  }

  override def addAppName(appName: String): Unit = {
    this.appName = appName
  }

  override def addEnvironmentName(envName: String): Unit = {
    this.envName = envName
  }

  override def addAppDuration(appStarted: Instant, appFinished: Instant): Unit = {
    this.appStarted = appStarted
    this.appFinished = appFinished
  }

  override def addDryRun(isDryRun: Boolean): Unit = {
    this.isDryRun = isDryRun
  }

  override def addUndercover(isUndercover: Boolean): Unit = {
    this.isUndercover = isUndercover
  }

  override def addRpsMetrics(minRps: Int, goodRps: Int): Unit = {
    this.minRps = minRps
    this.goodRps = goodRps
  }

  def addCompletedTask(completedTask: TaskResult): Unit = {
    completedTasks += completedTask
  }

  def renderSubject(): String = {
    val timeCreatedStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))
    val dryRunStr = if (isDryRun) "(DRY RUN) " else ""
    if (appException.isEmpty && !completedTasks.exists(t => t.runStatus.isFailure)) {
      s"${dryRunStr}Notification for $appName at $timeCreatedStr"
    } else {
      s"${dryRunStr}Notification of FAILURE for $appName at $timeCreatedStr"
    }
  }

  def renderBody(): String = {
    val builder = new MessageBuilderHtml

    renderHeader(builder)

    renderCompletedTasks(builder)

    val allSchemaChanges = completedTasks
      .flatMap(_.schemaChanges)
      .sortBy(_.tableName)

    renderSchemaDifference(builder, allSchemaChanges.toSeq)

    completedTasks
      .foreach(t => {
        t.runStatus match {
          case Failed(ex)           => renderJobException(builder, t, ex)
          case ValidationFailed(ex) => renderJobException(builder, t, ex)
          case _                    => // Do nothing
        }
      })

    builder.withRawParagraph(
      s"""Regards,<br>
         |Pramen<br>
         |${BuildProperties.getFullVersion}
         |""".stripMargin
    )

    builder.renderBody
  }

  private def renderHeader(builder: MessageBuilder): MessageBuilder = {
    val introParagraph = ParagraphBuilder()

    if (isDryRun)
      introParagraph.withText("(DRY RUN) ", Style.Bold)

    introParagraph
      .withText("This is a notification from Pramen for ")
      .withText(appName, Style.Bold)
      .withText(" on ")
      .withText(envName, Style.Bold)
      .withText(". The job has ")

    val someTasksSucceeded = completedTasks.exists(_.runStatus.isInstanceOf[Succeeded])
    val someTasksFailed = completedTasks.exists(t => t.runStatus.isFailure)

    appException match {
      case None if someTasksFailed && !someTasksSucceeded => introParagraph.withText("FAILED", Style.Error)
      case None if someTasksSucceeded && someTasksFailed  => introParagraph.withText("partially succeeded", Style.Warning)
      case None                                           => introParagraph.withText("succeeded", Style.Success)
      case Some(_)                                        => introParagraph.withText("FAILED", Style.Error)
    }

    introParagraph.withText(".")

    val jobStartedStr = ZonedDateTime.ofInstant(appStarted, zoneId).format(timestampFmt)
    val jobFinishedStr = ZonedDateTime.ofInstant(appFinished, zoneId).format(timestampFmt)
    val jobDurationMillis = Duration.between(appStarted, appFinished).toMillis

    val jobDurationParagraph = ParagraphBuilder()
      .withText("Job started at ")
      .withText(jobStartedStr, Style.Bold)
      .withText(", finished at ")
      .withText(jobFinishedStr, Style.Bold)
      .withText(". Elapsed time: ")
      .withText(TimeUtils.prettyPrintElapsedTime(jobDurationMillis), Style.Bold)
      .withText(".")

    if (isUndercover)
      jobDurationParagraph.withText(" The job ran in <i>undercover</i> mode - no updates to bookkeeping tables are saved.")

    builder.withParagraph(ParagraphBuilder().withText("Hi,"))
      .withParagraph(introParagraph)
      .withParagraph(jobDurationParagraph)

    appException.foreach(ex => builder.withException("The job has failed with the following exception:", ex))

    builder
  }

  private def getZoneId: ZoneId = {
    ConfigUtils.getOptionString(conf, TIMEZONE) match {
      case Some(tz) => ZoneId.of(tz)
      case None     => ZoneId.systemDefault()
    }
  }

  private def renderJobException(builder: MessageBuilder, taskResult: TaskResult, ex: Throwable): MessageBuilder = {
    val paragraphBuilder = ParagraphBuilder()
      .withText("Job ", Style.Exception)
      .withText(taskResult.job.name, Style.Error)
      .withText(" outputting to ", Style.Exception)
      .withText(taskResult.job.outputTable.name, Style.Error)

    taskResult.runInfo.foreach(info =>
      paragraphBuilder
        .withText(" at ", Style.Exception)
        .withText(info.infoDate.toString, Style.Error)
    )

    paragraphBuilder
      .withText(" has failed with an exception: ", Style.Exception)
      .withText(ex.getMessage, Style.Error)

    builder.withParagraph(paragraphBuilder)
    renderException(builder, ex)
  }

  private def renderException(builder: MessageBuilder, ex: Throwable): MessageBuilder = {
    val text = ex match {
      case CmdFailedException(msg, logLines) =>
        if (logLines.isEmpty) {
          msg
        } else {
          s"""$msg\nLast log lines:\n${logLines.mkString("", EOL, EOL)}"""
        }
      case ProcessFailedException(msg, stdout, stderr) =>
        val stdoutMsg = if (stdout.isEmpty) "" else s"""Last <b>stdout</b> lines:\n${stdout.mkString("", EOL, EOL)}"""
        val stderrMsg = if (stderr.isEmpty) "" else s"""Last <b>stderr</b> lines:\n${stderr.mkString("", EOL, EOL)}"""
        s"$msg\n$stdoutMsg\n$stderrMsg"
      case ex: Throwable                     =>
        val base = s"""${ex.toString}\n${ex.getStackTrace.map(s => s"  $s").mkString("", EOL, EOL)}"""
        val cause = Option(ex.getCause) match {
          case Some(c) => s"""\nCaused by ${c.toString}\n${c.getStackTrace.map(s => s"    $s").mkString("", EOL, EOL)}"""
          case None    => ""
        }
        base + cause
    }

    builder.withUnformattedText(text)
  }

  private def renderCompletedTasks(builder: MessageBuilder): MessageBuilder = {
    val sortedTasks = completedTasks
      .filter(t => t.runStatus != NotRan)
      .toArray
      //.sortBy(t => (t.job.name, t.job.outputTable.name, t.runInfo.map(_.infoDate.toString).getOrElse("")))

    if (sortedTasks.isEmpty) {
      builder.withParagraph("No new data has been loaded.")
    } else {
      renderTaskTable(builder, sortedTasks)
    }
  }

  private def renderTaskTable(builder: MessageBuilder, tasks: Seq[TaskResult]): MessageBuilder = {
    val outputRecordsKnown = tasks.exists(t => t.runStatus match {
      case _: Succeeded => true
      case _            => false
    })

    val outputSizeKnown = tasks.exists(t => t.runStatus match {
      case s: Succeeded => s.sizeBytes.isDefined
      case _            => false
    })

    val haveReasonColumn = tasks.exists(t => t.runStatus.isFailure || t.dependencyWarnings.nonEmpty)

    val tableBuilder = new TableBuilderHtml

    val tableHeaders = new ListBuffer[TableHeader]

    tableHeaders.append(TableHeader(TextElement("Job"), Align.Left))
    tableHeaders.append(TableHeader(TextElement("Table"), Align.Left))
    tableHeaders.append(TableHeader(TextElement("Date"), Align.Center))
    if (outputRecordsKnown)
      tableHeaders.append(TableHeader(TextElement("Record Count"), Align.Right))
    tableHeaders.append(TableHeader(TextElement("Elapsed Time"), Align.Center))
    if (outputSizeKnown)
      tableHeaders.append(TableHeader(TextElement("Size"), Align.Right))
    tableHeaders.append(TableHeader(TextElement("RPS"), Align.Right))
    tableHeaders.append(TableHeader(TextElement("Saved at"), Align.Center))
    tableHeaders.append(TableHeader(TextElement("Status"), Align.Center))
    if (haveReasonColumn)
      tableHeaders.append(TableHeader(TextElement("Reason"), Align.Left))

    tableBuilder.withHeaders(tableHeaders.toSeq)

    tasks.foreach(task => {
      val row = new ListBuffer[TextElement]

      row.append(TextElement(task.job.name))
      row.append(TextElement(task.job.outputTable.name))
      row.append(TextElement(task.runInfo.map(_.infoDate.toString).getOrElse("")))

      if (outputRecordsKnown)
        row.append(TextElement(getRecordCountText(task)))

      row.append(TextElement(getElapsedTime(task)))

      if (outputSizeKnown)
        row.append(TextElement(getOutputSize(task)))

      row.append(getThroughputRps(task))
      row.append(TextElement(getFinishTime(task)))
      row.append(getStatus(task))

      if (haveReasonColumn)
        row.append(TextElement(getFailureReason(task)))

      tableBuilder.withRow(row)
    })

    builder.withTable(tableBuilder)
  }

  private def getThroughputRps(task: TaskResult): TextElement = {
    val recordCount = task.runStatus match {
      case s: Succeeded => s.recordCount
      case _            => 0
    }

    task.runInfo match {
      case Some(runInfo) =>
        val jobDuration = Duration.between(runInfo.started, runInfo.finished).getSeconds
        if (jobDuration > MIN_RPS_JOB_DURATION_SECONDS && recordCount > 0L) {
          val throughput = recordCount / jobDuration

          throughput match {
            case n if n < minRps   => TextElement(throughput.toString, Style.Warning)
            case n if n >= goodRps => TextElement(throughput.toString, Style.Success)
            case _                 => TextElement(throughput.toString)
          }
        } else
          TextElement("")
      case None          => TextElement("")
    }
  }

  private def getRecordCountText(task: TaskResult): String = {
    task.runStatus match {
      case s: Succeeded =>
        val old = s.recordCountOld.getOrElse(0L)
        if (old == 0) {
          s.recordCount.toString
        } else {
          val difference = s.recordCount - old
          if (difference > 0) {
            s"${s.recordCount} (+$difference)"
          } else {
            s"${s.recordCount} ($difference)"
          }
        }
      case _            => ""
    }
  }

  private def getElapsedTime(task: TaskResult): String = {
    task.runInfo match {
      case Some(runInfo) => TimeUtils.prettyPrintElapsedTimeShort((runInfo.finished.getEpochSecond - runInfo.started.getEpochSecond) * 1000L)
      case _             => ""
    }
  }

  private def getOutputSize(task: TaskResult): String = {
    task.runStatus match {
      case s: Succeeded =>
        s.sizeBytes match {
          case Some(sizeBytes) => StringUtils.prettySize(sizeBytes)
          case None            => ""
        }
      case _            => ""
    }
  }

  private def getFailureReason(task: TaskResult): String = {
    task.runStatus match {
      case Failed(ex)                  => ex.getMessage
      case ValidationFailed(ex)        => ex.getMessage
      case MissingDependencies(tables) => s"Dependent job failures: ${tables.mkString(", ")}"
      case FailedDependencies(deps)    => s"Dependency check failures: ${deps.map(_.renderText).mkString("; ")}"
      case _                           =>
        if (task.dependencyWarnings.isEmpty) {
          ""
        } else {
          val tables = task.dependencyWarnings.map(_.table).sortBy(identity).mkString(", ")
          s"Optional dependencies failed for: $tables"
        }
    }
  }

  private def getFinishTime(task: TaskResult): String = {
    task.runInfo match {
      case Some(runInfo) => ZonedDateTime.ofInstant(runInfo.finished, zoneId).format(timestampFmt)
      case None          => ""
    }
  }

  private def getStatus(task: TaskResult): TextElement = {
    val successStyle = if (task.dependencyWarnings.isEmpty) Style.Success else Style.Warning

    task.runStatus match {
      case s: Succeeded           =>
        if (s.reason == TaskRunReason.Update) {
          TextElement("Update", Style.Warning)
        } else if (s.reason == TaskRunReason.Rerun) {
          TextElement("Rerun", successStyle)
        } else if (s.reason == TaskRunReason.Late) {
          TextElement("Late", Style.Warning)
        } else {
          TextElement("Success", successStyle)
        }
      case NoData                 => TextElement("No Data", Style.Warning)
      case _: Skipped             => TextElement("Skipped", successStyle)
      case NotRan                 => TextElement("Skipped", Style.Warning)
      case _: ValidationFailed    => TextElement("Validation failed", Style.Warning)
      case _: MissingDependencies => TextElement("Skipped", Style.Warning)
      case _: FailedDependencies  => TextElement("Skipped", Style.Warning)
      case _                      => TextElement("Failed", Style.Exception)
    }
  }

  private def renderSchemaDifference(builder: MessageBuilder, schemaDifferences: Seq[SchemaDifference]): MessageBuilder = {
    if (schemaDifferences.isEmpty) {
      return builder
    }

    builder.withParagraph(ParagraphBuilder()
      .withText("Warning! The following schema changes have been detected:", Style.Warning)
    )

    val tableBuilder = new TableBuilderHtml

    tableBuilder.withHeaders(Seq(
      TableHeader(TextElement("Table"), Align.Left),
      TableHeader(TextElement("Change"), Align.Center),
      TableHeader(TextElement("Old column"), Align.Center),
      TableHeader(TextElement("New column"), Align.Center),
      TableHeader(TextElement("Previous schema"), Align.Center),
      TableHeader(TextElement("Updated at"), Align.Center)
    ))

    for {
      diff <- schemaDifferences
      change <- diff.changes
    } {
      val changeCell = change match {
        case _: FieldChange.NewField     => TextElement("Added", Style.Success)
        case _: FieldChange.DeletedField => TextElement("Deleted", Style.Error)
        case _: FieldChange.ChangedType  => TextElement("Changed", Style.Warning)
      }

      val oldColumnCell = change match {
        case _: FieldChange.NewField     => TextElement("")
        case c: FieldChange.DeletedField => TextElement(s"<b>${c.columnName}</b> (${c.dataType})")
        case c: FieldChange.ChangedType  => TextElement(s"<b>${c.columnName}</b> (${c.oldType})")
      }

      val newColumnCell = change match {
        case c: FieldChange.NewField     => TextElement(s"<b>${c.columnName}</b> (${c.dataType})")
        case _: FieldChange.DeletedField => TextElement("")
        case c: FieldChange.ChangedType  => TextElement(s"<b>${c.columnName}</b> (${c.newType})")
      }

      tableBuilder.withRow(Seq(
        TextElement(diff.tableName),
        changeCell,
        oldColumnCell,
        newColumnCell,
        TextElement(diff.infoDateOld.toString),
        TextElement(diff.infoDateNew.toString)
      ))
    }

    builder.withTable(tableBuilder)
  }
}
