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
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.notification._
import za.co.absa.pramen.api.status.RunStatus._
import za.co.absa.pramen.api.status._
import za.co.absa.pramen.api.{FieldChange, SchemaDifference}
import za.co.absa.pramen.core.config.Keys.TIMEZONE
import za.co.absa.pramen.core.exceptions.{CmdFailedException, ProcessFailedException}
import za.co.absa.pramen.core.notify.message._
import za.co.absa.pramen.core.notify.pipeline.PipelineNotificationBuilderHtml.{MIN_MEGABYTES, MIN_RPS_JOB_DURATION_SECONDS, MIN_RPS_RECORDS}
import za.co.absa.pramen.core.utils.JvmUtils.getShortExceptionDescription
import za.co.absa.pramen.core.utils.StringUtils.renderThrowable
import za.co.absa.pramen.core.utils.{BuildPropertyUtils, ConfigUtils, StringUtils, TimeUtils}

import java.time._
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer

object PipelineNotificationBuilderHtml {
  val MIN_RPS_JOB_DURATION_SECONDS = 60
  val MIN_RPS_RECORDS = 1000
  val MIN_MEGABYTES = 10
  val NOTIFICATION_REASON_MAX_LENGTH_KEY = "pramen.notifications.reason.max.length"
  val NOTIFICATION_EXCEPTION_MAX_LENGTH_KEY = "pramen.notifications.exception.max.length"
}

class PipelineNotificationBuilderHtml(implicit conf: Config) extends PipelineNotificationBuilder {
  import PipelineNotificationBuilderHtml._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val timestampFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm Z")

  private val EOL = java.lang.System.lineSeparator

  private val zoneId = getZoneId

  private var minRps = 0
  private var goodRps = Int.MaxValue

  private val maxReasonLength = ConfigUtils.getOptionInt(conf, NOTIFICATION_REASON_MAX_LENGTH_KEY)
  private val maxExceptionLength = ConfigUtils.getOptionInt(conf, NOTIFICATION_EXCEPTION_MAX_LENGTH_KEY)

  var appException: Option[Throwable] = None
  var warningFlag: Boolean = false
  var appName: String = "Unspecified Job"
  var sparkAppId: Option[String] = None
  var envName: String = "Unspecified Environment"
  var appStarted: Instant = Instant.now()
  var appFinished: Instant = Instant.now()
  var isDryRun = false
  var isUndercover = false
  var customSignature = Seq.empty[TextElement]

  val completedTasks = new ListBuffer[TaskResult]
  val pipelineNotificationFailures = new ListBuffer[PipelineNotificationFailure]
  val customEntries = new ListBuffer[NotificationEntry]

  override def addFailureException(ex: Throwable): Unit = {
    appException = Option(ex)
  }

  override def addWarningFlag(flag: Boolean): Unit = {
    warningFlag = flag
  }

  override def addAppName(appName: String): Unit = {
    this.appName = appName
  }

  override def addSparkAppId(sparkAppId: String): Unit = {
    this.sparkAppId = Option(sparkAppId)
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

  override def addCompletedTask(completedTask: TaskResult): Unit = {
    completedTasks += completedTask
  }

  override def addPipelineNotificationFailure(failure: PipelineNotificationFailure): Unit = {
    pipelineNotificationFailures += failure
  }

  override def addCustomEntries(entries: Seq[NotificationEntry]): Unit = customEntries ++= entries

  override def addSignature(signature: TextElement*): Unit = customSignature = signature

  def renderSubject(): String = {
    val timeCreatedStr = ZonedDateTime.now(zoneId).format(timestampFmt)

    val dryRunStr = if (isDryRun) "(DRY RUN) " else ""

    pipelineStatus match {
      case PipelineStatus.Success        => s"${dryRunStr}Notification of SUCCESS for $appName at $timeCreatedStr"
      case PipelineStatus.Warning        => s"${dryRunStr}Notification of WARNING for $appName at $timeCreatedStr"
      case PipelineStatus.PartialSuccess => s"${dryRunStr}Notification of PARTIAL SUCCESS for $appName at $timeCreatedStr"
      case PipelineStatus.Failure        => s"${dryRunStr}Notification of FAILURE for $appName at $timeCreatedStr"
    }
  }

  def renderBody(): String = {
    val builder = new MessageBuilderHtml(conf)

    renderHeader(builder)

    renderCompletedTasks(builder)

    val allSchemaChanges = completedTasks
      .flatMap(_.schemaChanges)
      .sortBy(_.tableName)

    renderSchemaDifference(builder, allSchemaChanges.toSeq)

    renderCustomEntries(builder, customEntries)

    completedTasks
      .foreach(t => {
        t.runStatus match {
          case Failed(ex)           => renderJobException(builder, t, ex)
          case ValidationFailed(ex) => renderJobException(builder, t, ex)
          case _                    => // Do nothing
        }
      })

    renderPipelineNotificationFailures(builder)

    val notificationTargetErrors = completedTasks.flatMap(_.notificationTargetErrors)

    if (notificationTargetErrors.nonEmpty) {
      renderNotificationTargetErrors(builder, notificationTargetErrors)
    }

    renderSignature(builder)

    builder.renderBody
  }

  def pipelineStatus: PipelineStatus = {
    val isCertainFailure = appException.nonEmpty
    val (someTasksSucceeded, someTasksFailed) = getSuccessFlags
    val hasAtLeastOneWarning = warningFlag || hasWarnings

    if (isCertainFailure) {
      PipelineStatus.Failure
    } else if (!someTasksFailed && !hasAtLeastOneWarning) {
      PipelineStatus.Success
    } else if (someTasksSucceeded && someTasksFailed) {
      PipelineStatus.PartialSuccess
    } else if (someTasksSucceeded && hasAtLeastOneWarning) {
      PipelineStatus.Warning
    } else {
      PipelineStatus.Failure
    }
  }

  private[core] def renderHeader(builder: MessageBuilder): MessageBuilder = {
    val introParagraph = ParagraphBuilder()

    if (isDryRun)
      introParagraph.withText("(DRY RUN) ", Style.Bold)

    introParagraph
      .withText("This is a notification from Pramen for ")
      .withText(appName, Style.Bold)
      .withText(" on ")
      .withText(envName, Style.Bold)
      .withText(". The job has ")

    pipelineStatus match {
      case PipelineStatus.Success        => introParagraph.withText("succeeded", Style.Success)
      case PipelineStatus.Warning        => introParagraph.withText("succeeded with warnings", Style.Warning)
      case PipelineStatus.PartialSuccess => introParagraph.withText("partially succeeded", Style.Warning)
      case PipelineStatus.Failure        => introParagraph.withText("FAILED", Style.Error)
    }

    introParagraph.withText(".")

    val applicationIds = getSparkApplicationIds

    // This handles the case when all tasks are run under the same Spark Session.
    // When Pramen support runners that run tasks in different Spark Sessions (via Yarn, Glue etc APIs), this will need
    // to be revisited with adding application_id to the table of task results.
    if (applicationIds.length == 1) {
         introParagraph.withText(" Application ID: ")
        .withText(applicationIds.head, Style.Bold)
        .withText(".")
    }

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

  private[core] def getSparkApplicationIds: Seq[String] = {
    sparkAppId match {
      case Some(appId) => (appId +: completedTasks.map(_.applicationId.trim).filter(_.nonEmpty)).distinct.toSeq
      case None => completedTasks.map(_.applicationId.trim).filter(_.nonEmpty).distinct.toSeq
    }
  }

  private[core] def getSuccessFlags: (Boolean, Boolean) = {
    val hasNotificationFailures = completedTasks.exists(t => t.notificationTargetErrors.nonEmpty)
    val someTasksSucceeded = completedTasks.exists(_.runStatus.isInstanceOf[Succeeded]) && appException.isEmpty
    val someTasksFailed = completedTasks.exists(t => t.runStatus.isFailure) || hasNotificationFailures || appException.nonEmpty
    (someTasksSucceeded, someTasksFailed)
  }

  private[core] def hasWarnings: Boolean = {
    completedTasks.exists{task =>
      val hasDependencyWarnings = task.dependencyWarnings.nonEmpty
      val hasNotificationErrors = task.notificationTargetErrors.nonEmpty
      val hasTaskWarnings = task.runStatus.isInstanceOf[RunStatus.Succeeded] && task.runStatus.asInstanceOf[RunStatus.Succeeded].warnings.nonEmpty
      val hasSkippedWithWarnings = task.runStatus.isInstanceOf[RunStatus.Skipped] && task.runStatus.asInstanceOf[RunStatus.Skipped].isWarning
      val hasSchemaChanges = task.schemaChanges.nonEmpty
      val hasPipelineNotificationFailures = pipelineNotificationFailures.nonEmpty

      hasDependencyWarnings || hasNotificationErrors || hasTaskWarnings || hasSkippedWithWarnings || hasSchemaChanges || hasPipelineNotificationFailures
    }
  }

  private[core] def getZoneId: ZoneId = {
    ConfigUtils.getOptionString(conf, TIMEZONE) match {
      case Some(tz) => ZoneId.of(tz)
      case None     => ZoneId.systemDefault()
    }
  }

  private[core] def renderJobException(builder: MessageBuilder, taskResult: TaskResult, ex: Throwable): MessageBuilder = {
    val paragraphBuilder = ParagraphBuilder()
      .withText("Job ", Style.Exception)
      .withText(taskResult.jobName, Style.Error)
      .withText(" outputting to ", Style.Exception)
      .withText(taskResult.outputTable.name, Style.Error)

    taskResult.runInfo.foreach(info =>
      paragraphBuilder
        .withText(" at ", Style.Exception)
        .withText(info.infoDate.toString, Style.Error)
    )

    val errorMessage = ex.getMessage

    val errorMessageTruncated = maxReasonLength match {
      case Some(maxLength) if errorMessage.length > maxLength => errorMessage.substring(0, maxLength) + "..."
      case _ => errorMessage
    }

    paragraphBuilder
      .withText(" has failed with an exception: ", Style.Exception)
      .withText(errorMessageTruncated, Style.Error)

    builder.withParagraph(paragraphBuilder)
    renderException(builder, ex)
  }

  private[core] def renderException(builder: MessageBuilder, ex: Throwable): MessageBuilder = {
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
        renderThrowable(ex, maximumLength = maxExceptionLength)
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

    sortedTasks
      .filter(_.runStatus.isInstanceOf[RunStatus.Succeeded])
      .foreach(task => {
        val success = task.runStatus.asInstanceOf[RunStatus.Succeeded]
        if (success.filesRead.nonEmpty)
          renderFilesRead(builder, task, success)
      })

    builder
  }

  private[core] def renderTaskTable(builder: MessageBuilder, tasks: Seq[TaskResult]): MessageBuilder = {
    val outputRecordsKnown = tasks.exists(t => t.runStatus match {
      case _: Succeeded => true
      case _            => false
    })

    val outputSizeKnown = tasks.exists { t =>
      val hasExplicitSize = t.runStatus match {
        case s: Succeeded => s.sizeBytes.isDefined
        case _            => false
      }
      t.isRawFilesJob || hasExplicitSize
    }

    val haveReasonColumn = tasks.exists(t => t.runStatus.getReason.nonEmpty || t.dependencyWarnings.nonEmpty)
    val haveHiveColumn = tasks.exists(t => t.runStatus.isInstanceOf[Succeeded] && t.runStatus.asInstanceOf[Succeeded].hiveTablesUpdated.nonEmpty)

    val tableBuilder = new TableBuilderHtml

    val tableHeaders = new ListBuffer[TableHeader]

    tableHeaders.append(TableHeader(TextElement("Job"), Align.Left))
    tableHeaders.append(TableHeader(TextElement("Table"), Align.Left))
    if (haveHiveColumn)
      tableHeaders.append(TableHeader(TextElement("Catalog"), Align.Left))
    tableHeaders.append(TableHeader(TextElement("Date"), Align.Center))
    if (outputRecordsKnown)
      tableHeaders.append(TableHeader(TextElement("Record Count"), Align.Right))
    tableHeaders.append(TableHeader(TextElement("Elapsed Time"), Align.Center))
    if (outputSizeKnown)
      tableHeaders.append(TableHeader(TextElement("Size"), Align.Right))
    tableHeaders.append(TableHeader(TextElement("Throughput"), Align.Right))
    tableHeaders.append(TableHeader(TextElement("Saved at"), Align.Center))
    tableHeaders.append(TableHeader(TextElement("Status"), Align.Center))
    if (haveReasonColumn)
      tableHeaders.append(TableHeader(TextElement("Reason"), Align.Left))

    tableBuilder.withHeaders(tableHeaders.toSeq)

    tasks.foreach(task => {
      val row = new ListBuffer[TextElement]

      row.append(TextElement(task.jobName, getTransientTextStyle(task)))
      row.append(TextElement(task.outputTable.name, getTransientTextStyle(task)))

      if (haveHiveColumn) {
        val hiveTable = task.runStatus match {
          case s: Succeeded => s.hiveTablesUpdated
            .map(_.replace("`", ""))
            .mkString(", ")
          case _            => ""
        }

        row.append(TextElement(hiveTable))
      }

      row.append(TextElement(task.runInfo.map(_.infoDate.toString).getOrElse("")))

      if (outputRecordsKnown)
        row.append(TextElement(getRecordCountText(task)))

      row.append(TextElement(getElapsedTime(task)))

      if (outputSizeKnown) {
        if (task.isRawFilesJob) {
          row.append(TextElement(getSizeText(task)))
        } else {
          row.append(TextElement(getOutputSize(task)))
        }
      }

      row.append(getThroughputRps(task))
      row.append(TextElement(getFinishTime(task)))
      row.append(getStatus(task))

      if (haveReasonColumn)
        row.append(TextElement(getFailureReason(task)))

      tableBuilder.withRow(row)
    })

    builder.withTable(tableBuilder)
  }

  private[core] def renderPipelineNotificationFailures(builder: MessageBuilderHtml): MessageBuilder = {
    pipelineNotificationFailures.foreach { failure =>
      val notificationErrorsParagraph = ParagraphBuilder()
        .withText(s"Failed to send pipeline notification via '${failure.notificationTarget}': ", Style.Exception)
      builder.withParagraph(notificationErrorsParagraph)
      renderException(builder, failure.ex)
    }
    builder
  }

  private[core] def renderNotificationTargetErrors(builder: MessageBuilderHtml, notificationTargetErrors: ListBuffer[NotificationFailure]): MessageBuilder = {
    val tableBuilder = new TableBuilderHtml

    val tableHeaders = new ListBuffer[TableHeader]

    tableHeaders.append(TableHeader(TextElement("Notification target"), Align.Left))
    tableHeaders.append(TableHeader(TextElement("Table"), Align.Left))
    tableHeaders.append(TableHeader(TextElement("Date"), Align.Left))
    tableHeaders.append(TableHeader(TextElement("Error"), Align.Left))
    tableBuilder.withHeaders(tableHeaders.toSeq)

    notificationTargetErrors.foreach(error => {
      val row = new ListBuffer[TextElement]

      row.append(TextElement(error.notificationTarget))
      row.append(TextElement(error.table))
      row.append(TextElement(error.infoDate.toString))
      row.append(TextElement(getShortExceptionDescription(error.ex), Style.Error))

      tableBuilder.withRow(row)
    })

    val notificationErrorsParagraph = ParagraphBuilder()
      .withText("Failed to send notifications to the following targets:", Style.Exception)

    builder.withParagraph(notificationErrorsParagraph)
    builder.withTable(tableBuilder)
  }

  private[core] def renderFilesRead(builder: MessageBuilder, task: TaskResult, runStatus: RunStatus.Succeeded): MessageBuilder = {
    val tableBuilder = new TableBuilderHtml

    val tableHeaders = new ListBuffer[TableHeader]

    val taskName = s"Files sourced - ${task.outputTable.name} - ${task.runInfo.map(_.infoDate.toString).getOrElse(" ")}"

    tableHeaders.append(TableHeader(TextElement(taskName), Align.Left))
    tableBuilder.withHeaders(tableHeaders.toSeq)

    runStatus.filesRead.sorted.foreach(fileName => {
      val row = new ListBuffer[TextElement]

      row.append(TextElement(fileName))
      tableBuilder.withRow(row)
    })

    val emptyParagraph = ParagraphBuilder()
    builder.withParagraph(emptyParagraph)
    builder.withTable(tableBuilder)
  }

  private[core] def getThroughputRps(task: TaskResult): TextElement = {
    val recordCount = task.runStatus match {
      case s: Succeeded => s.recordCount
      case _            => 0
    }

    task.runInfo match {
      case Some(runInfo) =>
        val jobDuration = Duration.between(runInfo.started, runInfo.finished).getSeconds
        if (jobDuration > MIN_RPS_JOB_DURATION_SECONDS && recordCount >= MIN_RPS_RECORDS) {
          if (task.isRawFilesJob) {
            getBytesPerSecondsText(recordCount, jobDuration)
          } else {
            getRpsText(recordCount, jobDuration)
          }
        } else
          TextElement("")
      case None          => TextElement("")
    }
  }

  private[core] def getRpsText(recordOrByteCount: Long, jobDurationSeconds: Long): TextElement = {
    val throughput = recordOrByteCount / jobDurationSeconds
    val rps = s"${throughput.toString} r/s"

    throughput match {
      case n if n < minRps => TextElement(rps, Style.Warning)
      case n if n >= goodRps => TextElement(rps, Style.Success)
      case _ => TextElement(rps)
    }
  }

  private[core] def getBytesPerSecondsText(totalBytesCount: Long, jobDurationSeconds: Long): TextElement = {
    val MEGABYTE = 1024L * 1024L

    val sizeMb = totalBytesCount / MEGABYTE

    if (sizeMb < MIN_MEGABYTES) {
      TextElement("")
    } else {
      val throughput = totalBytesCount / jobDurationSeconds
      TextElement(s"${StringUtils.prettySize(throughput)}/s")
    }
  }

  private[core] def getRecordCountText(task: TaskResult): String = {
    def renderDifference(numRecords: Long, numRecordsOld: Option[Long]): String = {
      numRecordsOld match {
        case Some(old) if old > 0 =>
          val diff = numRecords - old
          if (diff > 0)
            s"$numRecords (+$diff)"
          else if (diff < 0)
            s"$numRecords ($diff)"
          else {
            numRecords.toString
          }
        case _ => numRecords.toString
      }
    }

    if (task.isRawFilesJob) {
      "-"
    } else {
      task.runStatus match {
        case s: Succeeded        => renderDifference(s.recordCount, s.recordCountOld)
        case d: InsufficientData => renderDifference(d.actual, d.recordCountOld)
        case _                   => ""
      }
    }
  }

  private[core] def getSizeText(task: TaskResult): String = {
    def renderDifferenceSize(numBytes: Long, numBytesOld: Option[Long]): String = {
      numBytesOld match {
        case Some(old) if old > 0 =>
          val diff = numBytes - old
          if (diff > 0)
            s"${StringUtils.prettySize(numBytes)} (+${StringUtils.prettySize(diff)})"
          else if (diff < 0)
            s"${StringUtils.prettySize(numBytes)} (-${StringUtils.prettySize(Math.abs(diff))})"
          else {
            StringUtils.prettySize(numBytes)
          }
        case _ => StringUtils.prettySize(numBytes)
      }
    }

    task.runStatus match {
      case s: Succeeded        => renderDifferenceSize(s.recordCount, s.recordCountOld)
      case d: InsufficientData => renderDifferenceSize(d.actual, d.recordCountOld)
      case _                   => ""
    }
  }

  private[core] def getElapsedTime(task: TaskResult): String = {
    task.runInfo match {
      case Some(runInfo) => TimeUtils.prettyPrintElapsedTimeShort((runInfo.finished.getEpochSecond - runInfo.started.getEpochSecond) * 1000L)
      case _             => ""
    }
  }

  private[core] def getOutputSize(task: TaskResult): String = {
    task.runStatus match {
      case s: Succeeded =>
        s.sizeBytes match {
          case Some(sizeBytes) => StringUtils.prettySize(sizeBytes)
          case None            => ""
        }
      case _            => ""
    }
  }

  private[core] def getFailureReason(task: TaskResult): String = {
    val reason = task.runStatus.getReason match {
      case Some(reason) => reason
      case None         =>
        if (task.dependencyWarnings.isEmpty) {
          ""
        } else {
          val tables = task.dependencyWarnings.map(_.table).sortBy(identity).mkString(", ")
          s"Optional dependencies failed for: $tables"
        }
    }

    maxReasonLength match {
      case Some(maxLength) if reason.length > maxLength =>  reason.substring(0, maxLength) + "..."
      case _ => reason
    }
  }

  private[core] def getFinishTime(task: TaskResult): String = {
    task.runInfo match {
      case Some(runInfo) => ZonedDateTime.ofInstant(runInfo.finished, zoneId).format(timestampFmt)
      case None          => ""
    }
  }

  private[core] def getStatus(task: TaskResult): TextElement = {
    val successStyle = if (task.dependencyWarnings.nonEmpty) Style.Warning else Style.Success

    task.runStatus match {
      case s: Succeeded           => getSuccessTextElement(s, task.dependencyWarnings.nonEmpty)
      case _: InsufficientData    => TextElement("Insufficient data", Style.Exception)
      case NoData(isFailure)      => TextElement("No Data", if (isFailure) Style.Exception else Style.Warning)
      case s: Skipped             => if (s.isWarning) TextElement("Skipped", Style.Warning) else TextElement("Skipped", successStyle)
      case NotRan                 => TextElement("Skipped", Style.Warning)
      case _: ValidationFailed    => TextElement("Validation failed", Style.Warning)
      case _: MissingDependencies => TextElement("Skipped", Style.Warning)
      case _: FailedDependencies  => TextElement("Skipped", Style.Warning)
      case _                      => TextElement("Failed", Style.Exception)
    }
  }

  private[core] def getSuccessTextElement(status: RunStatus.Succeeded, hasDependencyWarnings: Boolean): TextElement = {
    val successStyle = if (hasDependencyWarnings) Style.Warning else Style.Success

    val style = if (status.warnings.nonEmpty)
      Style.Warning
    else
      successStyle

    if (status.reason == TaskRunReason.Update) {
      TextElement("Update", Style.Warning)
    } else if (status.reason == TaskRunReason.Rerun) {
      if (status.warnings.nonEmpty)
        TextElement("Warning", style)
      else
        TextElement("Rerun", style)
    } else if (status.reason == TaskRunReason.Late) {
      TextElement("Late", Style.Warning)
    } else {
      if (status.warnings.nonEmpty)
        TextElement("Warning", style)
      else
        TextElement("Success", style)
    }
  }

  private[core] def renderSchemaDifference(builder: MessageBuilder, schemaDifferences: Seq[SchemaDifference]): MessageBuilder = {
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

  private def renderCustomEntries(builder: MessageBuilderHtml, customEntries: ListBuffer[NotificationEntry]): Unit = {
    customEntries.foreach {
      case NotificationEntry.Paragraph(text)       => builder.withParagraph(text)
      case NotificationEntry.Table(headers, cells) => builder.withTable(headers, cells)
      case NotificationEntry.UnorderedList(items)  => builder.withUnorderedList(items)
      case NotificationEntry.OrderedList(items)    => builder.withOrderedList(items)
      case NotificationEntry.UnformattedText(text) => builder.withUnformattedText(text)
      case NotificationEntry.Html(contents)        => builder.withHtmlText(contents)
      case _: NotificationEntry.AttachedFile       => // Skipping... This is going to be added elsewhere.
      case c                                       => log.error(s"Notification entry ${c.getClass} is not supported. Maybe this is related to Pramen runtime version mismatch.")
    }
  }

  private[core] def getTransientTextStyle(task: TaskResult): Style = {
    if (task.isTransient)
      Style.Italic
    else
      Style.Normal
  }

  def renderSignature(builder: MessageBuilder): MessageBuilder = {
    if (customSignature.isEmpty) {
      builder.withRawParagraph(
        s"""Regards,<br>
           |Pramen<br>
           |version ${BuildPropertyUtils.instance.getFullVersion}
           |""".stripMargin
      )
    } else {
      builder.withParagraph(customSignature)
    }
  }
}
